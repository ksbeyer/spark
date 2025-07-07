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
package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, OrderUtils}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableCommand, ColumnDefinition, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableChange.{ColumnChange, DeleteColumn, DropConstraint, UpdateColumnComment, UpdateColumnDefaultValue, UpdateColumnNullability, UpdateColumnPosition, UpdateColumnType}
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.connector.expressions.{ExternalExpression, FieldReference, NamedReference}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils


object GeneratedColumnV2 {

  /**
   * Prepare to compile [[org.apache.spark.sql.catalyst.plans.logical.GeneratedColumnDef]]s.
   * @return a function that compiles the optional
   *         [[org.apache.spark.sql.catalyst.plans.logical.GeneratedColumnDef]]
   *         of a [[ColumnDefinition]] to a validated [[GeneratedColumnSpec]].
   */
  def prepareGeneratedColumns(columns: Seq[ColumnDefinition],
                              catalog: TableCatalog,
                              ident: Identifier)
  : ColumnDefinition => Option[GeneratedColumnSpec] = {

    val generatedColumns = columns.filter(_.generationExpression.isDefined)
    if (generatedColumns.isEmpty) {
      // No generated columns to worry about
      return _ => None
    }

    if (generatedColumns.exists(_.generationExpression.get.virtual)) {
      if (!catalog.capabilities().contains(
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_VIRTUAL_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "virtual generated columns")
      }
    }
    if (generatedColumns.exists(_.generationExpression.get.stored)) {
      if (!catalog.capabilities().contains(
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "stored generated columns")
      }
    }

    val (baseCols, genCols) =
      columns.partition(col => col.generationExpression.isEmpty && col.identityColumnSpec.isEmpty)

    // todo: any issue here?
    //        do we need to worry about char/varchar here? why?
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      StructType(baseCols.map(_.toSimpleStructField)))
    val relation = LocalRelation(schema)

    col => {
      col.generationExpression.map { gcDef =>
        // Generated columns must support equality checks because the writer must be
        // able to enforce: <column value> <=> <generated expression>.
        val sql = gcDef.captured.sql
        if (!OrderUtils.isOrderable(col.dataType)) {
          throw unsupportedExpressionError(col.name, sql,
            s"Generated columns not supported for data type ${col.dataType.simpleString}")
        }
        val parsed = gcDef.captured.parsedExpr
        val analyzed = validate(col.name, sql, parsed, col.dataType, relation, genCols.map(_.name))
        // todo: do we want any rewrites on analyzed?
        val v2Expr = new V2ExpressionBuilder(analyzed).build().orNull
        val wrappedExpr = ExternalExpression.create(gcDef.captured.sql, v2Expr)
        new GeneratedColumnSpec(wrappedExpr, gcDef.virtual)
      }
    }
  }

  private def validate(name: String,
                       sql: String,
                       parsed: Expression,
                       dataType: DataType,
                       input: LogicalPlan,
                       generatedColumns: Seq[String]): Expression = {
    // No subqueries allowed
    // todo: SCALAR_SUBQUERY? SCALAR_SUBQUERY_REFERENCE, ...?
    // todo: we could allow subqueries, including aggregation and windowing
    //       as long as they don't reference non base fields.
    //       eg, last_purchase_time =
    //           (select max(a.ts) from values explode(actions) as a where a.type = 'purchase')
    if (parsed.containsPattern(PLAN_EXPRESSION)) {
      throw unsupportedExpressionError(name, sql, "subquery expressions are not allowed")
    }

    // Analyze the parsed result.  This will raise error for:
    //   Non-base column reference
    //   Non-builtin function
    //   Variable reference
    val analyzed = analyzeExpr(name, sql, parsed, input, generatedColumns)

    // todo: for stored GCs, nondet could be ok
    if (!analyzed.deterministic) {
      throw unsupportedExpressionError(name, sql, "expression must be deterministic")
    }

    // todo: what about context-dependent:
    //         CurrentTimestamp, CurrentUser, CurrentDatabase, CurrentSchema, ...?
    //   sql says blocked for gen cols, but customers are using...
    //   for constraints, they can be used, but only in limited way
    //   for defaults, they are ok.
    //   if (analyzed.containsPattern(CURRENT_LIKE)) {}

    if (!Cast.canUpCast(analyzed.dataType, dataType)) {
      throw unsupportedExpressionError(name, sql,
        s"generation expression data type ${analyzed.dataType.simpleString} " +
            s"is incompatible with column data type ${dataType.simpleString}")
    }

    // Blocking use of collations in generated columns (and constraints)
    // until we have a strategy to handle collation changes.
    if (analyzed.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      throw unsupportedExpressionError(name, sql,
        "generation expression cannot contain non utf8 binary collated string type")
    }

    analyzed
  }

  private def analyzePlan(plan: LogicalPlan): LogicalPlan = {
    val analyzer = GeneratedColumnAnalyzer
    // todo: need to control all semantics changing confs, like ansi mode
    val analyzed = analyzer.execute(plan)
    analyzer.checkAnalysis(analyzed)
    analyzed
  }

  // todo: this should be common with constraints and probably default values.
  // todo: this needs to force or block all semantics-affecting confs.
  //       They could be captured at the table level or be set globally.
  //       We also need a story for how more settings roll out.
  //       This conf management should be common with mat views.
  def analyzeExpr(
      name: String,
      sql: String,
      parsed: Expression,
      relation: LogicalPlan,
      generatedColumns: Seq[String]): Expression = {
    val plan = try {
      analyzePlan(Project(Seq(Alias(parsed, name)()), relation))
    } catch {
      case ex: AnalysisException =>
        // Improve error message if possible
        ex.getCondition match {
          case "UNRESOLVED_COLUMN.WITH_SUGGESTION" |
               "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION" =>
            ex.messageParameters.get("objectName").foreach { unresolvedCol =>
              val resolver = SQLConf.get.resolver

              // Whether `col` = `unresolvedCol` taking into account case-sensitivity
              def isUnresolvedCol(col: String) =
                resolver(unresolvedCol, QueryCompilationErrors.toSQLId(col))
              // Check whether the unresolved column is this column
              if (isUnresolvedCol(name)) {
                throw unsupportedExpressionError(name, sql,
                  "generation expression cannot reference itself")
              }
              // Check whether the unresolved column is another generated column in the schema
              if (generatedColumns.exists(isUnresolvedCol)) {
                throw unsupportedExpressionError(name, sql,
                  "generation expression cannot reference another generated column")
              }
            }
          case "UNRESOLVED_ROUTINE" =>
            // Cannot resolve function using built-in catalog
            ex.messageParameters.get("routineName").foreach { fnName =>
              throw unsupportedExpressionError(name, sql,
                s"failed to resolve $fnName to a built-in function")
            }
          case _ =>
        }
        throw ex
    }

    plan match {
      case Project(Seq(a: Alias), _) =>
        a.child
      case _ =>
        if (plan.containsPattern(TreePattern.AGGREGATE)) {
          throw unsupportedExpressionError(name, sql, s"aggregate functions are not allowed")
        }
        if (plan.containsPattern(TreePattern.WINDOW)) {
          throw unsupportedExpressionError(name, sql, s"window functions are not allowed")
        }
        // Are there other invalid plan patterns?
        throw unsupportedExpressionError(name, sql, s"feature not supported")
    }
  }

  private def unsupportedExpressionError(name: String,
                                         sql: String,
                                         reason: String): AnalysisException = {
    // todo: generalize for constraints
    new AnalysisException(
      errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      messageParameters = Map(
        "fieldName" -> name,
        "expressionStr" -> sql,
        "reason" -> reason))
  }

  /**
   * Simply parse the expression.
   * @param category The object type that stores the expression, either "Field" or "Constraint".
   * @param name The name of the object that stores the expression.
   * @param sql The spark sql text.
   * @return
   */
  def parseExpression(category: String,
                      name: String,
                      sql: String): Expression = {
    try {
      // todo: any problem with constructing parsers?
      //       afaik, they are not thread safe, even though I see parsers shared between threads
      //       eg GeneratedColumns.parser
      new CatalystSqlParser().parseExpression(sql)
    } catch {
      case e: ParseException =>
        // This is possible, eg when defined in a different spark version.
        // todo: add category to error
        throw unsupportedExpressionError(name, sql, e.getLocalizedMessage)
    }
  }

  /**
   * Verify column changes do not affect undropped generated columns and check constraints.
   */
  def validateExpressionsInAlterTable(command: AlterTableCommand,
                                      resolvedTable: ResolvedTable): Unit = {

    // Note: The ALTER TABLE commands limits the combinations of types of changes.
    // For example, we cannot drop constraints and columns at the same time, but this
    // code is prepared to handle it via getRemainingCheckConstraints.
    // todo: can this code be simpler because of this?
    val changes = command.changes

    // Gather the generated column and constraint expressions remaining after alter.
    val table = resolvedTable.table
    val generatedColumns = getRemainingGeneratedColumns(changes, table)
    val checkConstraints = getRemainingCheckConstraints(changes, table)
    if (generatedColumns.isEmpty && checkConstraints.isEmpty) {
      // no expressions to check
      return
    }

    val tableName = resolvedTable.name
    failIfPropertiesAltered(tableName, changes, generatedColumns)

    // find the fields that changed in a way that could affect expression evaluation.
    val directlyChanged = getChangedFields(changes)
    if (directlyChanged.isEmpty) {
      // no fields are changed, so expressions are ok
      return
    }

    val ancestorsOfChanged = getAncestors(directlyChanged)

    for (column <- generatedColumns) {
      val extExpr = column.generatedColumnSpec().expression()
      failIfAffected(tableName, directlyChanged, ancestorsOfChanged,
        "Column", column.name(), extExpr)
    }

    for (constraint <- checkConstraints) {
      // todo: use ExternalExpression in Check constraints.
      val extExpr = ExternalExpression.create(constraint.predicateSql(), constraint.predicate())
      failIfAffected(tableName, directlyChanged, ancestorsOfChanged,
        "Constraint", constraint.name(), extExpr)
    }
  }

  private def getRemainingGeneratedColumns(changes: Seq[TableChange], table: Table): Seq[Column] = {
    val droppedCols: Set[String] = changes.collect {
      case c: DeleteColumn if c.fieldNames().length == 1 => c.fieldNames()(0)
    }.toSet
    table.columns().toSeq.collect {
      case c if c.generatedColumnSpec() != null && !droppedCols(c.name()) => c
    }
  }

  private def getRemainingCheckConstraints(changes: Seq[TableChange], table: Table): Seq[Check] = {
    val droppedConstraints: Set[String] = changes.collect {
      case c: DropConstraint => c.name()
    }.toSet
    table.constraints().toSeq.collect {
      case c: Check if !droppedConstraints(c.name()) => c
    }
  }

  /**
   * Find the fields that are changing in a way that could affect generated columns or constraints.
   */
  private def getChangedFields(changes: Seq[TableChange]): Set[NamedReference] = {
    val directlyChanged = new mutable.HashSet[FieldReference]
    changes.foreach {
      case _: UpdateColumnDefaultValue |
           _: UpdateColumnComment |
           _: UpdateColumnPosition => // ignore
        //   _: UpdateColumnNullability =>
        // todo: I can't think of a a way that changing nullability can affect our expressions.
        //       so can we ignore it on dependencies?
      case c: ColumnChange =>
        //   AddColumn - adding a subfield could change expression on parent.
        //   DeleteColumn - could be used
        //   UpdateColumnType - potentially changes the result
        //   RenameColumn - involves changing the stored sql string / expr.
        // todo:  UpdateColumnNullability - cannot change stored results, as the current data
        //        is not changed. However, making a column not-null might cause
        directlyChanged += FieldReference(c.fieldNames().toSeq)
      case _ => // ignore non-column change
        // AlterTableCollation is ok because we block collation use in expressions.
        // todo: can Set/UnsetTableProperties affect expressions?

    }
    directlyChanged.toSet
  }

  /**
   * For each multi-part field, collect the ancestors, excluding self.
   */
  private def getAncestors(fields: Set[NamedReference]): Set[NamedReference] = {
    val ancestorsOfChanged = new mutable.HashSet[NamedReference]
    for (f <- fields) {
      val anc = ancestors(f)
      while (anc.hasNext && ancestorsOfChanged.add(anc.next())) {}
    }
    ancestorsOfChanged.toSet
  }

  /**
   * Return the strict ancestors of a path, not including self or empty.
   * For example, for a.b.c.d, this returns [a.b.c, a.b, a].
   */
  private def ancestors(reference: NamedReference): Iterator[NamedReference] = {
    val parts = reference.fieldNames()
    if (parts.length > 1) {
      parts.dropRight(1).inits.filter(_.nonEmpty).map(p => FieldReference(p.toSeq))
    } else {
      Iterator.empty
    }
  }


  private def failIfPropertiesAltered(tableName: String,
                                      changes: Seq[TableChange],
                                      generatedColumns: Seq[Column]): Unit = {
    val gcNames = generatedColumns.map(_.name()).toSet
    changes.collect {
      case c: UpdateColumnDefaultValue => c
      case c: UpdateColumnNullability => c
      case c: UpdateColumnType => c
    }.foreach { c =>
      val name = c.fieldNames().head
      if (gcNames.contains(name)) {
        throw new AnalysisException(
          errorClass = "ALTER_TABLE_CANNOT_ALTER_GENERATED_COLUMN",
          messageParameters = Map(
            "tableName" -> toSQLId(tableName),
            "columnName" -> toSQLId(name)))
      }
    }
  }

  /**
   * Raises exception if the expression is affected by the changed fields.
   *
   * Changes to subfields are tricksy. For example,
   *    add field A.B to field A.
   *       use of A.C is unaffected.
   *       to_json(A) is affected.
   *       so changing a subfield potentially changes all ancestor fields.
   *    delete field A.B
   *       A.C is unaffected
   *       to_json(A) is affected.
   *       A.B.D is affected
   *    change nullability of A
   *       A.C is may now be nullable
   *       So changing a superfield potentially changes all subfields.
   * So the right rule seems to be: todo: is this right?
   *     A use of field f is potentially affected iff
   *       - f in directlyChanged OR
   *       - any descendant of f in directlyChanged, ie, f in ancestorsOfChanged
   *       - any ancestor of f in directlyChanged OR
   */
  private def failIfAffected(tableName: String,
                             directlyChanged: Set[NamedReference],
                             ancestorsOfChanged: Set[NamedReference],
                             category: String, // Column or Constraint
                             name: String,
                             extExpr: ExternalExpression): Unit = {
    if (!extExpr.ok()) {
      throw new AnalysisException(
        errorClass = "ALTER_TABLE_AFFECTS_UNEVALUABLE_EXPRESSION",
        messageParameters = Map(
          "tableName" -> toSQLId(tableName),
          "category" -> category,
          "name" -> toSQLId(name)))
    }
    for (f <- getReferencedFields(category, name, extExpr)) {
      if (directlyChanged(f) || ancestorsOfChanged(f) || ancestors(f).exists(directlyChanged)) {
        throw new AnalysisException(
          errorClass = "ALTER_TABLE_AFFECTS_EXPRESSION",
          messageParameters =
            Map(
              "tableName" -> toSQLId(tableName),
              "columnName" -> toSQLId(f.fieldNames().toSeq),
              "category" -> category,
              "usedBy" -> toSQLId(name))
        )
      }
    }
  }

  /**
   * Return the fields referenced by this expression.
   * The expression is expected to be ok().
   * However, this may still throw a ParseException if the sql is invalid.
   */
  private def getReferencedFields(category: String,
                                  name: String,
                                  extExpr: ExternalExpression): Set[NamedReference] = {
    if (extExpr.getExpression != null) {
      extExpr.getExpression.references().toSet
    } else {
      // The expression can fail to parse if it uses spark sql from a different version.
      val parsed = parseExpression(category, name, extExpr.getSql)
      // todo: is it sufficient to get the unresolved attributes?
      //    Do we need to analyze with the base schema?
      //    Could references include anything other than UnresolvedAttribute?
      parsed.references.map { a =>
        FieldReference(a.asInstanceOf[UnresolvedAttribute].nameParts)
      }.toSet
    }
  }
}
