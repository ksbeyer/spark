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

package org.apache.spark.sql.catalyst.analysis

import scala.annotation.tailrec
import org.apache.spark.sql.catalyst.{EvaluateUnresolvedInlineTable, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, UnresolvedNamedLambdaVariable, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.catalyst.util.GeneratedColumn.{failIfGenerateAlways, isGeneratedAlways}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.{containsExplicitDefaultColumn, getDefaultOrIdentityOrThrow, isExplicitDefaultColumn}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2RelationBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField

/**
 * A virtual rule to resolve column "DEFAULT" in [[Project]] and [[UnresolvedInlineTable]] under
 * [[InsertIntoStatement]] and [[SetVariable]]. It's only used by the real rule `ResolveReferences`.
 *
 * This virtual rule is triggered if:
 * 1. The column "DEFAULT" can't be resolved normally by `ResolveReferences`. This is guaranteed as
 *    `ResolveReferences` resolves the query plan bottom up. This means that when we reach here to
 *    resolve the command, its child plans have already been resolved by `ResolveReferences`.
 * 2. The plan nodes between [[Project]] and command are all unary nodes that inherit the
 *    output columns from its child.
 * 3. The plan nodes between [[UnresolvedInlineTable]] and command are either
 *    [[Project]], or [[Aggregate]], or [[SubqueryAlias]].
 */
class ResolveColumnDefaultInCommandInputQuery(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {

  // TODO (SPARK-43752): support v2 write commands as well.
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case i: InsertIntoStatement if conf.enableDefaultColumns && i.table.resolved &&
        i.query.containsPattern(UNRESOLVED_ATTRIBUTE) &&
        willExpandDefaults(i.query) =>
      val staticPartCols = i.partitionSpec.filter(_._2.isDefined).keySet.map(normalizeFieldName)
      // For INSERT with static partitions, such as `INSERT INTO t PARTITION(c=1) SELECT ...`, the
      // input query schema should match the table schema excluding columns with static
      // partition values.
      val expectedQuerySchema = i.table.schema.filter { field =>
        !staticPartCols.contains(normalizeFieldName(field.name))
      }
      // Normally, we should match the query schema with the table schema by position. If the n-th
      // column of the query is the DEFAULT column, we should get the default value expression
      // defined for the n-th column of the table. However, if the INSERT has a column list, such as
      // `INSERT INTO t(b, c, a)`, the matching should be by name. For example, the first column of
      // the query should match the column 'b' of the table.
      // To simplify the implementation, `resolveColumnDefault` always does by-position match. If
      // the INSERT has a column list, we reorder the table schema w.r.t. the column list and pass
      // the reordered schema as the expected schema to `resolveColumnDefault`.

      // todo: is there any way query.output can fail, given the conditions of willFire?
      //   we need to delay firing until star-like expressions are expanded, along
      //   with
      val specifiedNames =
        if (i.byName) i.query.output.map(_.name)
        else i.userSpecifiedCols

      if (specifiedNames.isEmpty) {
        i.withNewChildren(Seq(resolveColumnDefault(i.query, expectedQuerySchema)))
      } else {
        val colNamesToFields: Map[String, StructField] =
          expectedQuerySchema.map { field =>
            normalizeFieldName(field.name) -> field
          }.toMap
        val orderedFields = specifiedNames.map { col =>
          colNamesToFields.getOrElse(normalizeFieldName(col), {
            val extraColumns =
              specifiedNames.filterNot(n => colNamesToFields.contains(normalizeFieldName(n))).
                  map(toSQLId).mkString(", ")
            throw QueryCompilationErrors.incompatibleDataToTableExtraColumnsError(
              i.table.asInstanceOf[DataSourceV2RelationBase].name,
              extraColumns = extraColumns)
          })
        }

        val withDefaults = resolveColumnDefault(i.query, orderedFields)

        if (i.byName) {
          i.copy(query = withDefaults)
        } else {
          // GENERATED ALWAYS fields are removed, so we remove their names too.
          val names = orderedFields.filterNot(GeneratedColumn.isGeneratedAlways).map(_.name)
          // todo: should we preserve original case of names?
          i.copy(userSpecifiedCols = names, query = withDefaults)
        }
      }

    case s: SetVariable if s.targetVariables.forall(_.isInstanceOf[VariableReference]) &&
        s.sourceQuery.containsPattern(UNRESOLVED_ATTRIBUTE) &&
        willExpandDefaults(s.sourceQuery) =>
      val expectedQuerySchema = s.targetVariables.map {
        case v: VariableReference =>
          StructField(v.identifier.name, v.dataType, v.nullable)
            .withCurrentDefaultValue(v.varDef.defaultValueSQL)
      }
      // We match the query schema with the SET variable schema by position. If the n-th
      // column of the query is the DEFAULT column, we should get the default value expression
      // defined for the n-th variable of the SET.
      s.withNewChildren(Seq(resolveColumnDefault(s.sourceQuery, expectedQuerySchema)))

    case _ => plan
  }

  @tailrec
  private def willExpandDefaults(plan: LogicalPlan): Boolean = {
    plan match {
      case a: SubqueryAlias => willExpandDefaults(a.child)
      case p: Project =>
        p.child.resolved &&
            attributesReady(p.projectList) &&
            willFire(p.projectList)
      case inlineTable: UnresolvedInlineTable =>
        willFire(inlineTable.expressions)
      case _ => false
    }
  }


  // todo: existing master code has issues:
  //       with case _: Project | _: Aggregate if acceptInlineTable =>
  //       and with Project with defaults recursing
  //   see https://databricks.slack.com/archives/D08NCPS5XPA/p1750876852122259
  // todo: also, drop support for DEFAULT in SELECT + (ORDER | LIMIT | OFFSET) ?
  //   it requires defaults to be in place to order on, but the defaults
  //   shouldn't be evaluated until after these operations, ie at insert.

  /**
   * Resolves the column "DEFAULT" in [[Project]] and [[UnresolvedInlineTable]]. A column is a
   * "DEFAULT" column if all the following conditions are met:
   * 1. The expression inside project list or inline table expressions is a single
   *    [[UnresolvedAttribute]] with name "DEFAULT". This means `SELECT DEFAULT, ...` is valid but
   *    `SELECT DEFAULT + 1, ...` is not.
   * 2. The project list or inline table expressions have less elements than the expected schema.
   *    To find the default value definition, we need to find the matching column for expressions
   *    inside project list or inline table expressions. This matching is by position and it
   *    doesn't make sense if we have more expressions than the columns of expected schema.
   * 3. The plan nodes between [[Project]] and [[InsertIntoStatement]] are
   *    all unary nodes that inherit the output columns from its child.
   * 4. The plan nodes between [[UnresolvedInlineTable]] and [[InsertIntoStatement]] are either
   *    [[Project]], or [[Aggregate]], or [[SubqueryAlias]].
   *
   *  The purpose is:
   *  1. Fill in expression for DEFAULT VALUE and "GENERATED BY DEFAULT AS IDENTITY".
   *  2. Validate GENERATED ALWAYS AS (expr | IDENTITY) columns are all default, and
   *     remove them. The will be filled in later by
   *     org.apache.spark.sql.catalyst.analysis.TableOutputResolver.resolveOutputColumns
   */
  private def resolveColumnDefault(
      plan: LogicalPlan,
      expectedQuerySchema: Seq[StructField]): LogicalPlan = {

    plan match {
      case a: SubqueryAlias =>
        a.mapChildren(resolveColumnDefault(_, expectedQuerySchema))

      case p: Project =>
        // guaranteed by caller: p.child.resolved && willFire(p.projectList)
        // todo: there is an ambiguity when p.child has a column named "default".
        //       the sql standard has as strict DEFAULT keyword
        //       and requires quoting to get a field named `default`.
        val newProjectList =
          expandDefaultInRow(p.projectList, expectedQuerySchema, (e, name) => Alias(e, name)())
        val newProj = p.copy(projectList = newProjectList, child = p.child)
        newProj.copyTagsFrom(p)
        newProj

      // todo: is this true UnresolvedInlineTable.rows.forall(_.length == names.length) ?
      //       add require or at least doc in UnresolvedInlineTable.
      //       and change check below to just check names.
      case inlineTable: UnresolvedInlineTable =>
        // guaranteed by caller: willFire(inlineTable.expressions)
        // better error if inline table is wonky
        EvaluateUnresolvedInlineTable.validateInputDimension(inlineTable)
        val unnamed = (e: Expression, _: String) => e
        val newRows = inlineTable.rows.map { row =>
          expandDefaultInRow(row, expectedQuerySchema, unnamed)
        }

        val (names, extraNames) = inlineTable.names.splitAt(expectedQuerySchema.length)
        val keepNames = names.zip(expectedQuerySchema).
            filterNot(p => isGeneratedAlways(p._2)).map(_._1) ++ extraNames
        val newInlineTable = inlineTable.copy(rows = newRows, names = keepNames)
        newInlineTable.copyTagsFrom(inlineTable)
        newInlineTable
    }
  }

  private def attributesReady(exprs: Seq[NamedExpression]): Boolean = {
    exprs.forall { e =>
      e.resolved || (
          e match {
            case _: Star |
                 _: MultiAlias |
                 _: UnresolvedAlias |
                 _: UnresolvedNamedLambdaVariable => false
            case _ => true
          })
    }
  }

  private def willFire(exprs: Seq[Expression]): Boolean = {
    // we will fire on something
    exprs.exists(containsExplicitDefaultColumn) &&
        // we are ready to fire
        exprs.forall(e => e.resolved || containsExplicitDefaultColumn(e))
  }


  private def expandDefaultInRow[T <: Expression](
      exprs: Seq[T],
      expectedQuerySchema: Seq[StructField],
      setName: (Expression, String) => T): Seq[T] = {
    val fields = expectedQuerySchema.iterator
    exprs.flatMap { e =>
      if (fields.hasNext) {
        val field = fields.next()
        e match {
          case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
            expandDefault(field).map(setName(_, u.name))
          case a @ Alias(u: UnresolvedAttribute, name) if isExplicitDefaultColumn(u) =>
            expandDefault(field).map(setName(_, name))
          case other =>
            nonDefault(other, field)
        }
      } else {
        unexpectedExpr(e)
      }
    }
  }

  private def expandDefault(field: StructField): Option[Expression] = {
    Option.when(!isGeneratedAlways(field)) { // Drop DEFAULT for GENERATE ALWAYS
      getDefaultOrIdentityOrThrow(field, useNullAsDefault = true)
    }
  }

  private def nonDefault[E <: Expression](expr: E, field: StructField): Option[E] = {
    if (containsExplicitDefaultColumn(expr)) {
      // todo: if (expr.isInstanceOf[UnresolvedAttribute])
      //       the column doesn't have a default
      //       or the default is not complex; it didn't match
      throw QueryCompilationErrors
          .defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList()
    }
    failIfGenerateAlways(field)
    Some(expr)
  }

  private def unexpectedExpr[T <: Expression](expr: T): Option[T] = {
    if (containsExplicitDefaultColumn(expr)) {
      // todo: better error: DEFAULT did not match a column
      throw QueryCompilationErrors
          .defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList()
    }
    Some(expr)
  }

  private def failIfContainsDefault(expr: Expression): Unit = {
  }

  /**
   * Normalizes a schema field name suitable for use in looking up into maps keyed by schema field
   * names.
   * @param str the field name to normalize
   * @return the normalized result
   */
  private def normalizeFieldName(str: String): String = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }
}
