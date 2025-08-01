/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.csv

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Provides access to CSV data from pure SQL statements.
 */
case class CSVFileFormat() extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "csv"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val parsedOptions = getCsvOptions(sparkSession, options)
    CSVDataSource(parsedOptions).isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = getCsvOptions(sparkSession, options)
    CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // This is a defensive check to ensure the schema doesn't have variant. It shouldn't be
    // triggered if other part of the code is correct because `supportDataType` doesn't allow
    // variant (in case the user is not using `supportDataType/supportReadDataType` correctly).
    dataSchema.foreach { field =>
      if (!supportDataType(field.dataType, allowVariant = false)) {
        throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError("CSV", field)
      }
    }
    val parsedOptions = getCsvOptions(sparkSession, options)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(job.getConfiguration, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new CsvOutputWriter(path, dataSchema, context, parsedOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        "." + parsedOptions.extension + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    val parsedOptions = getCsvOptions(sparkSession, options)
    val isColumnPruningEnabled = parsedOptions.isColumnPruningEnabled(requiredSchema)

    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    // Don't push any filter which refers to the "virtual" column which cannot present in the input.
    // Such filters will be applied later on the upper layer.
    val actualFilters =
      filters.filterNot(_.references.contains(parsedOptions.columnNameOfCorruptRecord))

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val actualDataSchema = StructType(
        dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
      val actualRequiredSchema = StructType(
        requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
      val parser = new UnivocityParser(
        actualDataSchema,
        actualRequiredSchema,
        parsedOptions,
        actualFilters)
      // Use column pruning when specified by Catalyst, except when one or more columns have
      // existence default value(s), since in that case we instruct the CSV parser to disable column
      // pruning and instead read each entire row in order to correctly assign the default value(s).
      val schema = if (isColumnPruningEnabled) actualRequiredSchema else actualDataSchema
      val isStartOfFile = file.start == 0
      val headerChecker = new CSVHeaderChecker(
        schema, parsedOptions, source = s"CSV file: ${file.urlEncodedPath}", isStartOfFile)
      CSVDataSource(parsedOptions).readFile(
        conf,
        file,
        parser,
        headerChecker,
        requiredSchema)
    }
  }

  override def toString: String = "CSV"

  /**
   * Allow reading variant from CSV, but don't allow writing variant into CSV. This is because the
   * written data (the string representation of variant) may not be read back as the same variant.
   */
  override def supportDataType(dataType: DataType): Boolean =
    supportDataType(dataType, allowVariant = false)

  override def supportReadDataType(dataType: DataType): Boolean =
    supportDataType(dataType, allowVariant = true)

  private def supportDataType(dataType: DataType, allowVariant: Boolean): Boolean = dataType match {
    case _: VariantType => allowVariant

    case _: TimeType => false
    case _: AtomicType => true

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }

  override def allowDuplicatedColumnNames: Boolean = true

  private def getCsvOptions(
      sparkSession: SparkSession,
      options: Map[String, String]): CSVOptions = {
    val conf = getSqlConf(sparkSession)
    new CSVOptions(
      options,
      conf.csvColumnPruning,
      conf.sessionLocalTimeZone,
      conf.columnNameOfCorruptRecord)
  }
}
