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
package org.apache.spark.sql.connector.expressions;

import org.apache.spark.SparkException;
import org.apache.spark.SparkException$;
import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.SparkThrowable;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.ExpressionNotEvaluable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 *
 * An expression stored in a catalog.
 * Used for default values, generated columns, check constraints, etc.
 * This may be either a Spark SQL string with and an optional v2 {@link Expression},
 * or an error: <ul>
 * <li> Error:<ul>
 *   <li> This expression is unevaluable.
 *   <li> The sql and expression are null.
 *   </ul>
 * <li> Both the Spark SQL string and v2 Expression: <ul>
 *   <li> They are assumed to have equivalent meaning.
 *   <li> This expression should be evaluable by this engine.
 *   <li> For evaluation, the v2 Expression is converted directly to a catalyst Expression.
 *         todo: do we need to keep consistent conversion of v2 Expression to both sql and catalyst?
 *               we could do just: sql -> catalyst -> v2 -> sql
 *               vs: sql -> catalyst <=> v2 -> sql
 *               or: sql <=> catalyst <=> v2 ; catalyst -> sql is used today, but seems dangerous...
 *   <li> For other uses, like v1 tables and describe commands, the sql string is used.
 *   </ul>
 * <li> Only Spark SQL string: <ul>
 *   <li> This expression is only evaluable by engines that understand the provided sql.
 *   <li> The sql may be from a different version of Spark, so it may be unevaluable.
 *   </ul>
 * </ul>
 * <p/>
 * The stored expression may have included a sql string and v2 Expression
 * that we did not understand, which can be caused by different spark versions.
 * In this case, we ignore the expression and proceed with the sql alone
 * <p/>
 * The stored expression may have a v2 expression without the Spark SQL, which
 * can be created by a non-spark sql engine.  If the expression is understood
 * and convertible to spark sql, we proceed with the sql and expression.
 * Otherwise, this instance be an error.
 * <p/>
 * The stored expression may have neither a v2 expression nor a Spark SQL,
 * which can be created by a non-spark sql engine using a foreign sql.
 * In this case, this instance will be an error.
 * <p/>
 *
 *  WARNING: Any changes between versions, may lead to incorrect results.
 *           For example, generated columns may not store the expected value,
 *           data may be incorrectly partitioned, or check constraints may be violated.
 *        todo: how do we fix semantics? ansi mode, and several more configs.
 *
 * @since 4.1.0
 */
@Evolving
public class ExternalExpression {
  private final String sql;
  private final Expression expr;
  private final String error;

  public static ExternalExpression create(@Nullable String sql,
                                          @Nullable Expression expr) {
    // todo: if sql != null but expr was recorded but not loadable,
    //       is continuing with the sql safe?  what it sql changed meaning
    //       and doesn't match.  Eg a new config setting changed behavior.
    //       Should we set error whenever v2 cannot be loaded?
    //       This problem exists with sql-only as well...
    //       Should we capture semantic configs, and if configs are not known, then set error?
    String error = null;
    if (sql == null) {
      // This is possible when an expression is defined by a non-spark engine.
      if (expr == null) {
        // This is possible when a v2 expression is not provided by the non-spark engine or
        // when the expression was not loadable.
        error = "Expression is not provided";
      } else {
        try {
          // todo: does this produce valid spark sql?
          sql = new V2ExpressionSQLBuilder().build(expr);
          if (sql == null) {
            // todo: is this possible?
            error = "Cannot translate v2 expression to SQL";
          }
        } catch (Throwable e) {
          // todo: can V2ExpressionSQLBuilder fail?  What exceptions can it throw?
          if (e instanceof SparkThrowable) {
            // todo: log?
            // todo: right message?
            error = e.getLocalizedMessage();
          } else {
            throw e;
          }
        }
      }
    }
    return new ExternalExpression(sql, expr, error);
  }

  public static ExternalExpression create(String sql) {
    return create(sql, null);
  }

  public static ExternalExpression unevaluable(String error) {
    return new ExternalExpression(null, null, error);
  }

  private ExternalExpression(String sql, Expression expr, String error) {
    assert(sql != null || error != null);
    this.sql = sql;
    this.expr = expr;
    this.error = error;
  }

  /**
   * True if we could not load the expression, so this is unevaluable.
   */
  public boolean ok() {
    return error == null;
  }

  public void checkError(Identifier tableName, String category, String name)
      throws ExpressionNotEvaluable {
    if (error != null) {
      throw new ExpressionNotEvaluable(tableName, category, name, error);
    }
  }

  private void checkError() {
    if (error != null) {
      throw new IllegalStateException("caller didn't check for error");
    }
  }

  /**
   * Returns the SQL representation of the expression (Spark SQL dialect).
   * This can only be called if ok() or after checkError().
   */
  public String getSql() {
    checkError();
    return sql;
  }

  /**
   * Returns the v2 expression representation of the expression.
   * This can only be called if ok() or after checkError().
   */
  @Nullable
  public Expression getExpression() {
    checkError();
    return expr;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    ExternalExpression that = (ExternalExpression) other;
    return Objects.equals(sql, that.sql) &&
        Objects.equals(expr, that.expr) &&
        Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, expr,  error);
  }

  @Override
  public String toString() {
    if (error != null) {
      return String.format("ExternalExpression{error=%s}", error);
    } else {
      return String.format("ExternalExpression{sql=%s, expression=%s}", sql, expr);
    }
  }
}
