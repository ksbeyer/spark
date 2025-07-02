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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AnalysisAwareExpression, Expression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.trees.TreePattern.{ANALYSIS_AWARE_EXPRESSION, TreePattern}
import org.apache.spark.sql.catalyst.util.{GeneratedColumn, IdentityColumn, V2ExpressionBuilder}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.validateDefaultValueExpr
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.{CURRENT_DEFAULT_COLUMN_METADATA_KEY, EXISTS_DEFAULT_COLUMN_METADATA_KEY}
import org.apache.spark.sql.connector.catalog.{ColumnDefaultValue, DefaultValue, IdentityColumnSpec}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StructField}

/**
 * Column definition for tables. This is an expression so that analyzer can resolve the default
 * value expression in DDL commands automatically.
 */
case class ColumnDefinition(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    comment: Option[String] = None,
    // todo: next 3 are mutually exclusive; should we make a hierarchy?
    defaultValue: Option[DefaultValueExpression] = None,
    generationExpression: Option[GeneratedColumnDef] = None,
    identityColumnSpec: Option[IdentityColumnSpec] = None,
    metadata: Metadata = Metadata.empty) extends Expression with Unevaluable {

  require( // todo: checked during AstBuilder. Eliminate later checks.
    Seq(defaultValue, generationExpression, identityColumnSpec).count(_.isDefined) <= 1,
    "A ColumnDefinition can have at most one of " +
      "default value, generated column, or an identity column.")

  override def children: Seq[Expression] = defaultValue.toSeq

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(defaultValue = newChildren.headOption.map(_.asInstanceOf[DefaultValueExpression]))
  }

  def toV1Column: StructField = {
    // todo: near duplicate code: CatalogV2Util.v2ColumnToStructField(toV2Column(""))
    val metadataBuilder = new MetadataBuilder().withMetadata(metadata)
    comment.foreach { c =>
      metadataBuilder.putString("comment", c)
    }
    defaultValue.foreach { default =>
      // For v1 CREATE TABLE command, we will resolve and execute the default value expression later
      // in the rule `DataSourceAnalysis`. We just need to put the default value SQL string here.
      metadataBuilder.putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, default.originalSQL)
      val existsSQL = default.child match {
        case l: Literal => l.sql
        case _ => default.originalSQL
      }
      metadataBuilder.putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, existsSQL)
    }
    generationExpression.foreach { ge =>
      require(!ge.virtual, "v1 tables do not support virtual columns") // todo: better error
      metadataBuilder.putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, ge.captured.sql)
    }
    encodeIdentityColumnSpec(metadataBuilder)
    StructField(name, dataType, nullable, metadataBuilder.build())
  }

  /**
   * Return the basic name and type without the column metadata.
   */
  def toSimpleStructField: StructField = StructField(name, dataType, nullable)

  private def encodeIdentityColumnSpec(metadataBuilder: MetadataBuilder): Unit = {
    identityColumnSpec.foreach { spec: IdentityColumnSpec =>
      metadataBuilder.putLong(IdentityColumn.IDENTITY_INFO_START, spec.getStart)
      metadataBuilder.putLong(IdentityColumn.IDENTITY_INFO_STEP, spec.getStep)
      metadataBuilder.putBoolean(
        IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
        spec.isAllowExplicitInsert)
    }
  }
}

object ColumnDefinition {

  // todo: this is nearly the same as
  //    org.apache.spark.sql.connector.catalog.CatalogV2Util.structFieldToV2Column
  //  yet they have some curious differences, eg other method does more validation.
  def fromV1Column(col: StructField, parser: ParserInterface): ColumnDefinition = {
    val metadataBuilder = new MetadataBuilder().withMetadata(col.metadata)
    metadataBuilder.remove("comment")
    metadataBuilder.remove(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
    metadataBuilder.remove(EXISTS_DEFAULT_COLUMN_METADATA_KEY)
    metadataBuilder.remove(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_START)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_STEP)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)

    val hasDefaultValue = col.getCurrentDefaultValue().isDefined &&
      col.getExistenceDefaultValue().isDefined
    val defaultValue = if (hasDefaultValue) {
      val defaultValueSQL = col.getCurrentDefaultValue().get
      Some(DefaultValueExpression(parser.parseExpression(defaultValueSQL), defaultValueSQL))
    } else {
      None
    }
    val generationExpr = GeneratedColumn.getGenerationExpression(col).map { sql =>
        // todo: set origin?
      val expr = parser.parseExpression(sql)
      GeneratedColumnDef(CapturedExpression(sql, expr), virtual = false)
    }
    val identityColumnSpec = if (col.metadata.contains(IdentityColumn.IDENTITY_INFO_START)) {
      Some(new IdentityColumnSpec(
        col.metadata.getLong(IdentityColumn.IDENTITY_INFO_START),
        col.metadata.getLong(IdentityColumn.IDENTITY_INFO_STEP),
        col.metadata.getBoolean(IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)
      ))
    } else {
      None
    }
    ColumnDefinition(
      col.name,
      col.dataType,
      col.nullable,
      col.getComment(),
      defaultValue,
      generationExpr,
      identityColumnSpec,
      metadataBuilder.build()
    )
  }

  // Called by `CheckAnalysis` to check column definitions in DDL commands.
  def checkColumnDefinitions(plan: LogicalPlan): Unit = {
    plan match {
      // Do not check anything if the children are not resolved yet.
      case _ if !plan.childrenResolved =>

      // Wrap errors for default values in a more user-friendly message.
      case cmd: V2CreateTablePlan if cmd.columns.exists(_.defaultValue.isDefined) =>
        val statement = cmd match {
          case _: CreateTable => "CREATE TABLE"
          case _: ReplaceTable => "REPLACE TABLE"
          case other =>
            val cmd = other.getClass.getSimpleName
            throw SparkException.internalError(
              s"Command $cmd should not have column default value expression.")
        }
        cmd.columns.foreach { col =>
          col.defaultValue.foreach { default =>
            checkDefaultColumnConflicts(col)
            validateDefaultValueExpr(default, statement, col.name, Some(col.dataType))
          }
        }

      case cmd: AddColumns if cmd.columnsToAdd.exists(_.default.isDefined) =>
        cmd.columnsToAdd.foreach { c =>
          c.default.foreach { d =>
            validateDefaultValueExpr(d, "ALTER TABLE ADD COLUMNS", c.colName, Some(c.dataType))
          }
        }

      case cmd: AlterColumns if cmd.specs.exists(_.newDefaultExpression.isDefined) =>
        cmd.specs.foreach { c =>
          c.newDefaultExpression.foreach { d =>
            validateDefaultValueExpr(d, "ALTER TABLE ALTER COLUMN", c.column.name.quoted,
              None)
          }
        }

      case _ =>
    }
  }

  private def checkDefaultColumnConflicts(col: ColumnDefinition): Unit = {
    // todo: this is guarded by assertion in ColumnDefinition, so not needed
    if (col.generationExpression.isDefined) {
      throw new AnalysisException(
        errorClass = "GENERATED_COLUMN_WITH_DEFAULT_VALUE",
        messageParameters = Map(
          "colName" -> col.name,
          "defaultValue" -> col.defaultValue.get.originalSQL,
          "genExpr" -> col.generationExpression.get.captured.sql
        )
      )
    }
    if (col.identityColumnSpec.isDefined) {
      throw new AnalysisException(
        errorClass = "IDENTITY_COLUMN_WITH_DEFAULT_VALUE",
        messageParameters = Map(
          "colName" -> col.name,
          "defaultValue" -> col.defaultValue.get.originalSQL,
          "identityColumnSpec" -> col.identityColumnSpec.get.toString
        )
      )
    }
  }
}

/**
 * A fake expression to hold the column/variable default value expression and its original SQL text.
 */
case class DefaultValueExpression(
    child: Expression,
    originalSQL: String,
    analyzedChild: Option[Expression] = None)
  extends UnaryExpression
  with Unevaluable
  with AnalysisAwareExpression[DefaultValueExpression] {

  final override val nodePatterns: Seq[TreePattern] = Seq(ANALYSIS_AWARE_EXPRESSION)

  override def dataType: DataType = child.dataType
  override def stringArgs: Iterator[Any] = Iterator(child, originalSQL)
  override def markAsAnalyzed(): DefaultValueExpression =
    copy(analyzedChild = Some(child))
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  // Convert the default expression to ColumnDefaultValue, which is required by DS v2 APIs.
  def toV2(statement: String, colName: String): ColumnDefaultValue = child match {
    case Literal(value, dataType) =>
      val currentDefault = analyzedChild.flatMap(new V2ExpressionBuilder(_).build())
      val existsDefault = LiteralValue(value, dataType)
      new ColumnDefaultValue(originalSQL, currentDefault.orNull, existsDefault)
    case _ =>
      throw QueryCompilationErrors.defaultValueNotConstantError(statement, colName, originalSQL)
  }

  // Convert the default expression to DefaultValue, which is required by DS v2 APIs.
  def toV2CurrentDefault(statement: String, colName: String): DefaultValue = child match {
    case Literal(_, _) =>
      val currentDefault = analyzedChild.flatMap(new V2ExpressionBuilder(_).build())
      new DefaultValue(originalSQL, currentDefault.orNull)
    case _ =>
      throw QueryCompilationErrors.defaultValueNotConstantError(statement, colName, originalSQL)
  }
}

/**
 * Captures an expression and its parsed Expression tree, but hides the expression
 * from analysis and rewrite.  The expression gets special analysis and additional handling
 * during DDL statements.  For example, this is used by generated columns during create table
 * commands.  We defer analysis to use a limited function catalog and to force configuration
 * a stable meaning of the expression (e.g. ansi mode).
 *
 * @param sql The original sql text
 * @param parsedExpr The parsed but unanalyzed expression tree.
 */
case class CapturedExpression(sql: String, parsedExpr: Expression)


/**
 * Defines a generated column as part of CREATE TABLE.
 * @param captured The original sql text and the parsed expression.
 * @param virtual true if the generated column is VIRTUAL, false if STORED.
 */
case class GeneratedColumnDef(captured: CapturedExpression,
                              virtual: Boolean) {
  require(!virtual, "virtual generated columns are not supported")
  def stored: Boolean = !virtual
}
