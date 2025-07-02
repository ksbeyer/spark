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

package org.apache.spark.sql.internal.connector

import javax.annotation.Nullable

import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue, GeneratedColumnSpec, IdentityColumnSpec}
import org.apache.spark.sql.types.DataType

// The standard concrete implementation of data source V2 column.
case class ColumnImpl(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    @Nullable comment: String, // nullable
    // todo: The next 3 are mutually exclusive; make hierarchy?
    @Nullable defaultValue: ColumnDefaultValue,
    @Nullable generatedColumnSpec: GeneratedColumnSpec,
    @Nullable identityColumnSpec: IdentityColumnSpec,
    @Nullable metadataInJSON: String)
    extends Column {
  require(Seq(defaultValue, generatedColumnSpec, identityColumnSpec).count(_ ne null) <= 1,
    "at most one of defaultValue, generatedColumnSpec, identityColumnSpec may be specified")
}
