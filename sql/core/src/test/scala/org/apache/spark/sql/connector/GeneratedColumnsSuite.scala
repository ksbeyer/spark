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

package org.apache.spark.sql.connector

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.{GeneratedColumnSpec, Identifier}
import org.apache.spark.sql.connector.expressions.{Cast, ExternalExpression, FieldReference, GeneralScalarExpression}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Tests for generated columns
 *
 * More generated columns tests:
 *  - [[DataSourceV2SQLSuite]]
 *  - [[org.apache.spark.sql.catalyst.parser.DDLParserSuite]]
 *  - [[org.apache.spark.sql.execution.command.DDLSuite]]
 *  - [[org.apache.spark.sql.collation.CollationSuite]]
 *  - [[org.apache.spark.rdd.JdbcRDDSuite]]
 *  - [[org.apache.spark.JavaJdbcRDDSuite]]
 */
class GeneratedColumnsSuite extends DatasourceV2SQLBase {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  private val cat = "testcat"

  test("simple insert generated column") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null,
          |                y int not null generated always as (-x))
          | using csv
          |""".stripMargin)

      // verify schema
      val table = catalog(cat).asTableCatalog.loadTable(Identifier.of(Array(), "t"))
      val columns = table.columns()
      assert(columns.length == 2)
      assert(columns(0).generatedColumnSpec() eq null)
      val spec1 = new GeneratedColumnSpec(
        ExternalExpression.create("-x",
          new GeneralScalarExpression("-",  // todo: is there a list of valid functions?
            // todo: are we headed to more function path ambiguities?
            Array(new FieldReference(Seq("x"))))),
        false)
      assertResult(spec1)(columns(1).generatedColumnSpec())

      // insert positionally without generated
      sql("insert into t values (3), (5), (7)")
      checkAnswer(spark.table("t"),
        Seq((3, -3), (5, -5), (7, -7)).toDF("x", "y"))

      // insert specified names without generated
      sql("insert OVERWRITE t (x) values (1), (2), (3)")
      checkAnswer(spark.table("t"),
        Seq((1, -1), (2, -2), (3, -3)).toDF("x", "y"))

      // insert positionally with generated
      sql("insert OVERWRITE t values (2, default), (4, default), (6, default)")
      checkAnswer(spark.table("t"),
        Seq((2, -2), (4, -4), (6, -6)).toDF("x", "y"))

      // insert by name with generated
      sql("insert OVERWRITE t (y, x) values (default, -1), (default, -2), (default, -3)")
      checkAnswer(spark.table("t"),
        Seq((-1, 1), (-2, 2), (-3, 3)).toDF("x", "y"))

      // insert by name with generated
      sql("insert OVERWRITE t by name select default as y, id + 1 as x from range(3)")
      checkAnswer(spark.table("t"),
        Seq((1, -1), (2, -2), (3, -3)).toDF("x", "y"))

      // insert by name without generated
      sql("insert OVERWRITE t by name select id + 1 as x from range(3)")
      checkAnswer(spark.table("t"),
        Seq((1, -1), (2, -2), (3, -3)).toDF("x", "y"))

      // cannot explicitly set generated column
      checkError(
        intercept[AnalysisException] {
          sql("insert OVERWRITE t values (3,1), (5,2), (7,3)")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "tableColumns" -> "`x`",
          "dataColumns" -> "`col1`, `col2`") )
    }
  }

  test("insert generated columns with select *") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null default 3,
          |                y int not null default 5,
          |                z int not null generated always as (x + y))
          | using csv
          |""".stripMargin)

      // positional
      sql("insert into t select * from (select id + 1, id + 11 from range(3))")
      checkAnswer(spark.table("t"),
        Seq((1, 11, 12), (2, 12, 14), (3, 13, 16)).toDF("x", "y", "z"))

      // positional + default value
      sql("insert OVERWRITE t select *, default from range(3)")
      checkAnswer(spark.table("t"),
        Seq((0, 5, 5), (1, 5, 6), (2, 5, 7)).toDF("x", "y", "z"))

      // positional + default value + default gc
      sql("insert OVERWRITE t select *, default, default from range(3)")
      checkAnswer(spark.table("t"),
        Seq((0, 5, 5), (1, 5, 6), (2, 5, 7)).toDF("x", "y", "z"))

      // positional + default gc
      sql("insert OVERWRITE t select *, default from (select id + 1, id + 11 from range(3))")
      checkAnswer(spark.table("t"),
        Seq((1, 11, 12), (2, 12, 14), (3, 13, 16)).toDF("x", "y", "z"))

      // positional + default value first
      sql("insert OVERWRITE t select default, *, default from range(3)")
      checkAnswer(spark.table("t"),
        Seq((3, 0, 3), (3, 1, 4), (3, 2, 5)).toDF("x", "y", "z"))

      // positional - but too many defaults
      checkError(
        intercept[AnalysisException] {
          sql("insert OVERWRITE t select *, default, default " +
              "from (select id, id + 10 from range(3))")
        },
        condition = "DEFAULT_PLACEMENT_INVALID",
        parameters = Map.empty
      )

      // specified + default value first
      sql("insert OVERWRITE t (z, x, y) select default, * " +
          "from (select id + 1, id + 11 from range(3))")
      checkAnswer(spark.table("t"),
        Seq((1, 11, 12), (2, 12, 14), (3, 13, 16)).toDF("x", "y", "z"))

      // specified + default value first
      sql("insert OVERWRITE t (z,y,x) select default, *, default from range(3)")
      checkAnswer(spark.table("t"),
        Seq((3, 0, 3), (3, 1, 4), (3, 2, 5)).toDF("x", "y", "z"))

      // by name - * = x
      sql("insert OVERWRITE t by name select * from range(3) as t(x)")
      checkAnswer(spark.table("t"),
        Seq((0, 5, 5), (1, 5, 6), (2, 5, 7)).toDF("x", "y", "z"))

      // by name - * = y
      sql("insert OVERWRITE t by name select * from range(3) as t(y)")
      checkAnswer(spark.table("t"),
        Seq((3, 0, 3), (3, 1, 4), (3, 2, 5)).toDF("x", "y", "z"))

      // todo: this incorrectly fails on master with
      //       [DEFAULT_PLACEMENT_INVALID] ... part of an expression.
      //    create table t(x int not null default 3, y int not null default 5);
      //    insert into t by name select *, default x from range(3) as t(y);
      //  push changes to ResolveColumnDefaultInCommandInputQuery with fix for above first?
      //    this also includes the changes to the recursion.

      // by name - * = y, default x
      sql("insert OVERWRITE t by name select *, default x from range(3) as t(y)")
      checkAnswer(spark.table("t"),
        Seq((3, 0, 3), (3, 1, 4), (3, 2, 5)).toDF("x", "y", "z"))

      // by name - * = y, default z
      sql("insert OVERWRITE t by name select *, default z from range(3) as t(y)")
      checkAnswer(spark.table("t"),
        Seq((3, 0, 3), (3, 1, 4), (3, 2, 5)).toDF("x", "y", "z"))

      // by name - * = x,y
      sql("insert OVERWRITE t by name select * from " +
          "(select id + 11 as y, id + 1 as x from range(3))")
      checkAnswer(spark.table("t"),
        Seq((1, 11, 12), (2, 12, 14), (3, 13, 16)).toDF("x", "y", "z"))

      // by name - default z, * = x,y
      sql("insert OVERWRITE t by name select default z, * from " +
          "(select id + 11 as y, id + 1 as x from range(3))")
      checkAnswer(spark.table("t"),
        Seq((1, 11, 12), (2, 12, 14), (3, 13, 16)).toDF("x", "y", "z"))

      // specified - but unknown column provided
      checkError(
        intercept[AnalysisException] {
          sql("insert OVERWRITE t (x, y, w, z) select *, default, default " +
              "from (select id as x, id + 10 as y from range(3))")
        },
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "extraColumns" -> "`w`"))

      // by name - but unknown column provided
      checkError(
        intercept[AnalysisException] {
          sql("insert OVERWRITE t by name select *, default as w, default as z " +
              "from (select id as x, id + 10 as y from range(3))")
        },
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "extraColumns" -> "`w`"))
    }
  }

  test("simple update generated column") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null,
          |                y int not null generated always as (-x))
          | using csv
          |""".stripMargin)

      // update is not supported yet
      checkError(
        intercept[SparkUnsupportedOperationException] {
          sql("update t set x = -2 * x where y <> 2")
          // checkAnswer(spark.table("t"),
          //   Seq((2, -2), (-2, 2), (6, -6)).toDF("x", "y"))
        },
        condition = "_LEGACY_ERROR_TEMP_2096",
        parameters = Map("ddl" -> "UPDATE TABLE")
      )

      // update is not supported yet -
      // but when it is, this should return error for setting generated column
      checkError(
        intercept[SparkUnsupportedOperationException] {
          sql("update t set y = -1 where x = 2")
        },
        condition = "_LEGACY_ERROR_TEMP_2096",
        parameters = Map("ddl" -> "UPDATE TABLE")
      )
    }
  }

  test("generated column on default value") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null default 86,
          |                y int not null generated always as (-x))
          | using csv
          |""".stripMargin)

      // insert positionally without generated, no default
      sql("insert into t values (3), (5), (7)")
      checkAnswer(spark.table("t"),
        Seq((3, -3), (5, -5), (7, -7)).toDF("x", "y"))

      // insert positionally without generated, with all default
      sql("insert OVERWRITE t values (default), (default), (default)")
      checkAnswer(spark.table("t"),
        Seq((86, -86), (86, -86), (86, -86)).toDF("x", "y"))

      // insert positionally without generated, with some default
      sql("insert OVERWRITE t values (1), (default), (3)")
      checkAnswer(spark.table("t"),
        Seq((1, -1), (86, -86), (3, -3)).toDF("x", "y"))

      // insert by name without generated, some default
      sql("insert OVERWRITE t (x) values (default), (2), (DEFAULT)")
      checkAnswer(spark.table("t"),
        Seq((86, -86), (2, -2), (86, -86)).toDF("x", "y"))

      // insert positionally with generated, some default
      sql("insert OVERWRITE t values (2, default), (default, default), (default, default)")
      checkAnswer(spark.table("t"),
        Seq((2, -2), (86, -86), (86, -86)).toDF("x", "y"))

      // insert by name with generated, some default
      sql("insert OVERWRITE t (y, x) values (default, -1), (default, -2), (default, default)")
      checkAnswer(spark.table("t"),
        Seq((-1, 1), (-2, 2), (86, -86)).toDF("x", "y"))

      // insert by name, all default or generated
      sql("insert OVERWRITE t by name (select default as x from range (3))")
      checkAnswer(spark.table("t"),
        Seq((86, -86), (86, -86), (86, -86)).toDF("x", "y"))

      // insert by name, default generated, no default
      sql("insert OVERWRITE t by name (select default as y, id as x from range (3))")
      checkAnswer(spark.table("t"),
        Seq((0, 0), (1, -1), (2, -2)).toDF("x", "y"))

      // insert by name, all default
      sql("insert OVERWRITE t(y, x) (select default as a, default as b from range (3))")
      checkAnswer(spark.table("t"),
        Seq((86, -86), (86, -86), (86, -86)).toDF("x", "y"))

      // insert by name, all default
      sql("insert OVERWRITE t(y, x) (select default as a, id as b from range (3))")
      checkAnswer(spark.table("t"),
        Seq((0, 0), (1, -1), (2, -2)).toDF("x", "y"))

      // This could be accepted, but is blocked today; parser requires non-empty name list.
      // If the parser is changed, this should insert 3 default rows.
      assertThrows[ParseException] {
        sql("insert OVERWRITE t () values (), (), ()")
      }
    }
  }

  test("many generated columns on many default values") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(a int not null default 76,
          |                b int not null default 86,
          |                c int not null default 96,
          |                x array<int> not null generated always as (array(a, b)),
          |                y array<int> not null generated always as (array(b, c)),
          |                z array<int> not null generated always as (array(a, b, c)))
          | using csv
          |""".stripMargin)

      // insert positionally without generated, no default
      sql("insert into t values (1,2,3), (4,5,6)")
      checkAnswer(spark.table("t"),
        Seq(
          (1, 2, 3, Array(1, 2), Array(2, 3), Array(1, 2, 3)),
          (4, 5, 6, Array(4, 5), Array(5, 6), Array(4, 5, 6))
        ).toDF("a", "b", "c", "x", "y", "z"))

      // insert positionally without generated, some default
      sql("insert overwrite t(b) values (1), (2)")
      checkAnswer(spark.table("t"),
        Seq(
          (76, 1, 96, Array(76, 1), Array(1, 96), Array(76, 1, 96)),
          (76, 2, 96, Array(76, 2), Array(2, 96), Array(76, 2, 96))
        ).toDF("a", "b", "c", "x", "y", "z"))

      // insert by name with some generated,some default
      sql("insert overwrite t(c, y) select id, default from range(2)")
      checkAnswer(spark.table("t"),
        Seq(
          (76, 86, 0, Array(76, 86), Array(86, 0), Array(76, 86, 0)),
          (76, 86, 1, Array(76, 86), Array(86, 1), Array(76, 86, 1))
        ).toDF("a", "b", "c", "x", "y", "z"))
    }
  }

  test("only generated columns") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null generated always as (5),
          |                y int not null generated always as (7))
          | using csv
          |""".stripMargin)

      // insert specified
      sql("insert into t(y) select default from range(2)")
      checkAnswer(spark.table("t"),
        Seq((5, 7), (5, 7)).toDF("x", "y"))

      // insert by-name
      sql("insert overwrite t by name select default as y from range(3)")
      checkAnswer(spark.table("t"),
        Seq((5, 7), (5, 7), (5, 7)).toDF("x", "y"))

      // insert positionally
      sql("insert overwrite t values (default, default), (default, default), (default, default)")
      checkAnswer(spark.table("t"),
        Seq((5, 7), (5, 7), (5, 7)).toDF("x", "y"))

      // insert positionally with empty rows
      sql("insert overwrite t values struct(), struct(), struct()")
      checkAnswer(spark.table("t"),
        Seq((5, 7), (5, 7), (5, 7)).toDF("x", "y"))

    }
  }

  test("generated column cannot refer to another generated column") {
    // We could support generated column on other generated columns using lateral columns.
    sql(s"USE $cat")
    withTable("t") {
      checkError(
        intercept[AnalysisException] {
          sql(
            """
              | create table t(x int not null,
              |                y int not null generated always as (-x),
              |                z int not null generated always as (-y))
              | using csv
              |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "z",
          "expressionStr" -> "-y",
          "reason" -> "generation expression cannot reference another generated column")
      )
    }
  }

  test("generated column cannot refer to itself") {
    sql(s"USE $cat")
    withTable("t") {
      checkError(
        intercept[AnalysisException] {
          sql(
            """
              | create table t(x int not null,
              |                y int not null generated always as (-y))
              | using csv
              |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "-y",
          "reason" -> "generation expression cannot reference itself")
      )
    }
  }

  test("generated column cannot refer to identity") {
    // We could support generated column on identity columns with relatively little effort.
    sql(s"USE $cat")
    withTable("t") {
      checkError(
        intercept[AnalysisException] {
          sql(
            """
              | create table t(x int not null generated by default as identity,
              |                y int not null generated always as (-x))
              | using csv
              |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "-x",
          "reason" -> "generation expression cannot reference another generated column")
      )
    }
  }

  test("generated column cannot use subqueries") {
    sql(s"USE $cat")
    withTable("t", "u") {
      sql(
        """
          | create table u(x int not null)
          | using csv
          |""".stripMargin)

      checkError(
        intercept[AnalysisException] {
          sql(
            """
              | create table t(x int not null,
              |                y int not null generated always as (x in (select x from u)))
              | using csv
              |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "x in (select x from u)",
          "reason" -> "subquery expressions are not allowed")
      )
    }
  }

  test("generated column must be deterministic") {
    sql(s"USE $cat")
    withTable("t") {
      checkError(
        intercept[AnalysisException] {
          sql(
            """
              | create table t(x int not null,
              |                y int not null generated always as (random()))
              | using csv
              |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "random()",
          "reason" -> "expression must be deterministic")
      )
    }
  }

  test("generated column ... current-like") {
    // todo: sql says current-like should be blocked.  It currently works, and is used by customers.
    //    it definitely needs to be blocked for virtual columns.
    //    but should we block it for stored columns?
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(x int not null,
          |                y string not null generated always as (current_catalog()))
          | using csv
          |""".stripMargin)

      sql("insert into t values (1), (2), (3)")
      checkAnswer(spark.table("t"),
        Seq((1, cat), (2, cat), (3, cat)).toDF("x", "y"))
    }
  }

  test("generated column cannot use non-builtins functions") {
    withUserDefinedFunction("foo" -> false) {
      sql(
        """
          | create function foo(x int) return x + 1;
          |""".stripMargin)

      checkError(
        intercept[AnalysisException] {
          sql(
            s"""
               | create table $cat.t(x int not null,
               |                y int not null generated always as (foo(x)))
               | using csv
               |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "foo(x)",
          "reason" -> "failed to resolve `foo` to a built-in function")
      )
    }
  }

  test("generated column cannot use temp functions") {
    withUserDefinedFunction("foo" -> true) {
      sql(
        """
          | create temporary function foo(x int) return x + 1;
          |""".stripMargin)

      checkError(
        intercept[AnalysisException] {
          sql(
            s"""
               | create table $cat.t(x int not null,
               |                     y int not null generated always as (foo(x)))
               | using csv
               |""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "y",
          "expressionStr" -> "foo(x)",
          "reason" -> "failed to resolve `foo` to a built-in function")
      )
    }
  }

  test("generated column cannot use global variables") {
    withSessionVariable("foo") {
      sql("declare variable foo int = 17")
      val query =
        s"""
           | create table $cat.t(x int not null,
           |                     y int not null generated always as (foo + x))
           | using csv
           |""".stripMargin
      checkError(
        intercept[AnalysisException] {
          sql(query)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`foo`", "proposal" -> "`x`"),
        context = ExpectedContext(
          fragment = "foo",
          start = 98,
          stop = 100
        )
      )
    }
  }

  test("generated column cannot use aggregates or windows") {
    checkError(
      intercept[AnalysisException] {
        sql(
          s"""
             | create table $cat.t(x int not null,
             |                     y int not null generated always as (sum(x)))
             | using csv
             |""".stripMargin)
      },
      condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "y",
        "expressionStr" -> "sum(x)",
        "reason" -> "aggregate functions are not allowed"))

    checkError(
      intercept[AnalysisException] {
        sql(
          s"""
             | create table $cat.t(x int not null,
             |                     y int not null generated always as (row_number() over (order by x)))
             | using csv
             |""".stripMargin)
      },
      condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "y",
        "expressionStr" -> "row_number() over (order by x)",
        "reason" -> "window functions are not allowed"))

    checkError(
      intercept[AnalysisException] {
        sql(
          s"""
             | create table $cat.t(x int not null,
             |                     y int not null generated always as (sum(x) over ()))
             | using csv
             |""".stripMargin)
      },
      condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      parameters = Map(
        "fieldName" -> "y",
        "expressionStr" -> "sum(x) over ()",
        "reason" -> "window functions are not allowed"))

    // A window function that parses, but fails to analyze
    checkError(
      intercept[AnalysisException] {
        sql(
          s"""
             | create table $cat.t(x int not null,
             |                     y int not null generated always as (row_number() over ()))
             | using csv
             |""".stripMargin)
      },
      condition = "_LEGACY_ERROR_TEMP_1037",
      parameters = Map("wf" -> "row_number()"))
  }


  test("generated column expression requires parens") {
    checkError(
      intercept[ParseException] {
        sql(
          s"""
             | create table $cat.t(x int not null,
             |                     y int not null generated always as -x)
             | using csv
             |""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'-'", "hint" -> "")
    )
  }

  test("generated columns are blocked on v1") {
    // using default spark_catalog
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""
             | create table t(x int not null,
             |                y int not null generated always as (-x))
             | using csv
             |""".stripMargin)
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`spark_catalog`.`default`.`t`",
        "operation" -> "generated columns"
      )
    )
  }

  test("alter table with generated columns") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(a int not null default 86,
          |                b int not null default 87,
          |                c int not null default 98,
          |                d int not null default 89,
          |                x int not null generated always as (a + b),
          |                y int not null generated always as (17),
          |                z int not null generated always as (b + d))
          | using csv
          |""".stripMargin)

      for (change <- Seq(
        "type long", "set not null", "drop not null", "set default 3", "drop default")) {
        // scalastyle:off println
        println(change)
        // scalastyle:on println
        checkError(
          intercept[AnalysisException] {
            sql(s"alter table t alter column x $change")
          },
          condition = "ALTER_TABLE_CANNOT_ALTER_GENERATED_COLUMN",
          parameters = Map(
            "tableName" -> s"`$cat`.`t`",
            "columnName" -> "`x`")
        )
      }

      checkError(
        intercept[AnalysisException] {
          sql("alter table t rename column b to b1")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> s"`$cat`.`t`",
          "columnName" -> "`b`",
          "category" -> "Column",
          "usedBy" -> "`x`")
      )

      checkError(
        intercept[AnalysisException] {
          sql("alter table t drop column (d)")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> s"`$cat`.`t`",
          "columnName" -> "`d`",
          "category" -> "Column",
          "usedBy" -> "`z`")
      )

      // move generated column is ok
      checkAnswer(
        sql("alter table t alter column c first"),
        Seq())

      // Rename unused is ok
      checkAnswer(
        sql("alter table t rename column c to c1"),
        Seq())

      // rename gen col is ok
      checkAnswer(
        sql("alter table t rename column z to z1"),
        Seq())

      // dropping used column is ok if dropped with referencing column.
      checkAnswer(
        sql("alter table t drop columns (z1, d)"),
        Seq())

      // todo: more to test?
    }
  }


  test("generated columns on struct fields") {
    sql(s"USE $cat")
    withTable("t") {
      sql(
        """
          | create table t(a struct<
          |                    b: struct<c: int, d: int>,
          |                    e: struct<f: int, g: int> not null,
          |                    h: struct<i: int not null, j: int>> not null,
          |                w int generated always as (a.e.g),
          |                x string generated always as (cast(a as string)),
          |                y string generated always as (cast(a.e as string)),
          |                z string generated always as (cast(a.h as string)),
          |                r struct<i: long, s: long> generated always as (struct(a.h.j as s, a.h.i as t)) )
          | using parquet
          |""".stripMargin)

      // verify schema
      val table = catalog(cat).asTableCatalog.loadTable(Identifier.of(Array(), "t"))
      val columns = table.columns()
      val aSchema = columns(0).dataType().asInstanceOf[StructType]
      assert(columns.length == 6)
      assert(columns(0).generatedColumnSpec() eq null)
      val specW = new GeneratedColumnSpec(
        ExternalExpression.create("a.e.g",
            new FieldReference(Seq("a", "e", "g"))),
        false)
      assertResult(specW)(columns(1).generatedColumnSpec())
      val specX = new GeneratedColumnSpec(
        ExternalExpression.create("cast(a as string)",
          // todo: why is input type needed on cast? better to add a type assertion expr?
          new Cast(new FieldReference(Seq("a")), aSchema, StringType)),
        false)
      assertResult(specX)(columns(2).generatedColumnSpec())
      val specY = new GeneratedColumnSpec(
        ExternalExpression.create("cast(a.e as string)",
          new Cast(new FieldReference(Seq("a", "e")), aSchema.fields(1).dataType, StringType)),
        false)
      assertResult(specY)(columns(3).generatedColumnSpec())
      val specZ = new GeneratedColumnSpec(
        ExternalExpression.create("cast(a.h as string)",
          new Cast( new FieldReference(Seq("a", "h")), aSchema.fields(2).dataType, StringType)),
        false)
      assertResult(specZ)(columns(4).generatedColumnSpec())
      val specR = new GeneratedColumnSpec(
        ExternalExpression.create("struct(a.h.j as s, a.h.i as t)"),
        false)
      assertResult(specR)(columns(5).generatedColumnSpec())

      // insert by name without generated and verify insert works
      sql("insert into t by name " +
          "select struct(struct(2 as d, 1 as c) as b," +
          "              struct(6 as j, 5 as i) as h," +
          "              struct(4 as g, 3 as f) as e) as a")
      val expect1 = new GenericRowWithSchema(
        Array(
          Row(Row(1, 2), Row(3, 4), Row(5, 6)),
          4,
          "{{1, 2}, {3, 4}, {5, 6}}",
          "{3, 4}", "{5, 6}",
          Row(6, 5)),
        aSchema)
      checkAnswer(spark.table("t"), expect1)

      // cannot drop a.b.c because a is used by field x
      checkError(
        intercept[AnalysisException] {
          sql("alter table t drop column (a.b.c)")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "columnName" -> "`a`",
          "category" -> "Column",
          "usedBy" -> "`x`")
      )

      // cannot rename a.b.c because a is used by field x
      checkError(
        intercept[AnalysisException] {
          sql("alter table t rename column a.b.c to c1")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "columnName" -> "`a`",
          "category" -> "Column",
          "usedBy" -> "`x`")
      )

      // dropping used column is ok if dropped with referencing column.
      checkAnswer(
        sql("alter table t drop column (a.b.c, x)"),
        Seq()
      )

      // renaming now unused column is ok.
      checkAnswer(
        sql("alter table t rename column a.b to b1"),
        Seq()
      )

      // cannot rename a.e.f because a.e is used by field y
      checkError(
        intercept[AnalysisException] {
          sql("alter table t rename column a.e.f to f1")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "columnName" -> "`a`.`e`",
          "category" -> "Column",
          "usedBy" -> "`y`")
      )

      // cannot rename a.e because it is used by field w
      checkError(
        intercept[AnalysisException] {
          sql("alter table t rename column a.e to e1")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "columnName" -> "`a`.`e`.`g`",
          "category" -> "Column",
          "usedBy" -> "`w`")
      )

      // cannot rename a because it is used by field w
      checkError(
        intercept[AnalysisException] {
          sql("alter table t rename column a to a1")
        },
        condition = "ALTER_TABLE_AFFECTS_EXPRESSION",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "columnName" -> "`a`.`e`.`g`",
          "category" -> "Column",
          "usedBy" -> "`w`")
      )

      // todo: more to test? check coverage
    }
  }

  test("schema with ctas is blocked") {
    // generated column isn't actually relevant to the test today - any schema is blocked,
    // but if ctas supports schemas someday, we want generated columns to be considered.
    sql(s"USE $cat")
    val query =
      """create table t (x int not null,
        |                 y int not null generated always as (-x))
        | using csv
        | as select id as x from range(3)""".stripMargin
    checkError(
      intercept[AnalysisException] {
        sql(query)
      },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map(
        "message" -> "Schema may not be specified in a Create Table As Select (CTAS) statement"),
      context = ExpectedContext(fragment = query, start = 0, stop = 132)
    )
  }


  // blocked on v1 also in DDLSuite.scala
  // blocked on v2 catalog without GC feature in DataSourceV2SQLSuite.scala

  // todo: test ExternalExpression
  //   sql + expr
  //   sql-only
  //   sql with ansi mode, other semantic configs - need to do work to enforce
  //       block def and use when configs not set to expected?
  //       force configs while analyzing configs?  Do any configs affect parsing / AstBuilder?

  // todo: test gen col ExternalExpression is error:
  //    query is ok
  //    insert fails
  //    update fails
  //    alter fails under assumption that unresolved might affect.
  //   repeat for ExternalExpression sql does not parse.
  //   repeat for ExternalExpression sql parses but fails to resolve.
  //   repeat for ExternalExpression expr fails to translate
  //   repeat for ExternalExpression expr translates but fails to resolve
  //  repeat all of this for constraints

  // todo: find or add tests for
  //     catalyst -> v2 Expression
  //     v2 Expression -> spark sql
  //     v2 Expression -> catalyst, or replace path with  v2 Expression -> spark sql -> catalyst.
  // todo: if we replace the path, can driver use of v2 expressions be opaque to this layer?
  // todo: related, can we store validated catalyst in ExternalExpression?

  // todo: incomparable types are not supported: eg, variant.
  // todo: can upcast
  // todo: must upcast

  // todo: bug on master
  //   create table t(
  //      a struct<b: struct<c: int, d: int>, e: int> not null
  //        default struct(struct(2 as d, 3 as c) as b, 3 as e))
  //   insert into t by name values (default) as t(a);
  //   repeat 3 times
  //   25/07/01 17:50:36 WARN ResolveDefaultColumns: Encountered unresolved exists default value:
  //    'NAMED_STRUCT('b', NAMED_STRUCT('d', 2, 'c', 3), 'e', 3)' for column a
  //    with StructType(StructField(b,StructType(StructField(c,IntegerType,true),
  //      StructField(d,IntegerType,true)),true),StructField(e,IntegerType,true)),
  //      falling back to full analysis.
  //   why message?
  //   message repeats once per insert... why?
  //   why doesn't by_name muck with structs?
  //       in org.apache.spark.sql.catalyst.analysis.TableOutputResolver.reorderColumnsByName
  //       where does UpCast go on defaults?

}
