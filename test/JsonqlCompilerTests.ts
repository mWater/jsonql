// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import SqlFragment from "../src/SqlFragment"
import SchemaMap from "../src/SchemaMap"
import JsonqlCompiler from "../src/JsonqlCompiler"
import { JsonQLExpr, JsonQLQuery } from "../src"

// Capitalizes tables and columns and aliases
class MockSchemaMap extends SchemaMap {
  async mapTable(table: any) {
    return new SqlFragment(table.toUpperCase())
  }

  // Map a column reference of a table aliased as alias
  async mapColumn(table: any, column: any, alias: any) {
    return new SqlFragment(alias + "." + column.toUpperCase())
  }

  mapTableAlias(alias: any) {
    return "a_" + alias
  }
}

var compiler: JsonqlCompiler

describe("JsonqlCompiler", async function () {
  beforeEach(function () {
    compiler = new JsonqlCompiler(new MockSchemaMap())
  })

  it("compiles simple query", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select ? as "x" from ABC as "a_abc1"')
    assert.deepEqual(compiled.params, [4])
  })

  it("compiles distinct query", async function () {
    const query: JsonQLQuery = {
      type: "query",
      distinct: true,
      selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select distinct ? as "x" from ABC as "a_abc1"')
    assert.deepEqual(compiled.params, [4])
  })

  it("compiles query with null select", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select null from ABC as "a_abc1"')
  })

  it("compiles query with field", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x" from ABC as "a_abc1"')
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with where", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" },
      where: {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "abc1", column: "q" },
          { type: "literal", value: 5 }
        ]
      }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x" from ABC as "a_abc1" where (a_abc1.Q > ?)')
    assert.deepEqual(compiled.params, [5])
  })

  it("compiles query with groupBy ordinals", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" },
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ],
      from: { type: "table", table: "abc", alias: "abc1" },
      groupBy: [1, 2]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" group by 1, 2')
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with groupBy expr", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" },
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ],
      from: { type: "table", table: "abc", alias: "abc1" },
      groupBy: [{ type: "field", tableAlias: "abc1", column: "p" }, 2]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" group by a_abc1.P, 2')
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with orderBy ordinal", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" },
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ],
      from: { type: "table", table: "abc", alias: "abc1" },
      orderBy: [{ ordinal: 1, direction: "desc" }, { ordinal: 2 }]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" order by 1 desc, 2')
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with orderBy ordinal with nulls", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" },
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ],
      from: { type: "table", table: "abc", alias: "abc1" },
      orderBy: [{ ordinal: 1, direction: "desc", nulls: "first" }, { ordinal: 2 }]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(
      compiled.sql,
      'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" order by 1 desc nulls first, 2'
    )
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with orderBy expr", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" },
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ],
      from: { type: "table", table: "abc", alias: "abc1" },
      orderBy: [{ expr: { type: "field", tableAlias: "abc1", column: "q" } }]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" order by a_abc1.Q')
    assert.deepEqual(compiled.params, [])
  })

  it("compiles query with limit", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" },
      limit: 10
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select ? as "x" from ABC as "a_abc1" limit ?')
    assert.deepEqual(compiled.params, [4, 10])
  })

  it("compiles query with offset", async function () {
    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" },
      offset: 10
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select ? as "x" from ABC as "a_abc1" offset ?')
    assert.deepEqual(compiled.params, [4, 10])
  })

  it("compiles query with subquery query", async function () {
    const subquery: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 5 }, alias: "q" }],
      from: { type: "table", table: "xyz", alias: "xyz1" }
    }

    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "x" }],
      from: { type: "subquery", query: subquery, alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1."q" as "x" from (select ? as "q" from XYZ as "a_xyz1") as "a_abc1"')
    assert.deepEqual(compiled.params, [5])
  })

  it("compiles query with subexpression", async function () {
    const subexpr: JsonQLExpr = {
      type: "op",
      op: "json_array_elements",
      exprs: [{ type: "literal", value: [{ a: 1 }, { a: 2 }] }]
    }

    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "abc1" }, alias: "x" }],
      from: { type: "subexpr", expr: subexpr, alias: "abc1" }
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(compiled.sql, 'select a_abc1 as "x" from json_array_elements(?) as "a_abc1"')
    assert.deepEqual(compiled.params, [[{ a: 1 }, { a: 2 }]])
  })

  it("compiles query with withs", async function () {
    const withQuery: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 5 }, alias: "q" }],
      from: { type: "table", table: "xyz", alias: "xyz1" }
    }

    const query: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "x" }],
      from: { type: "table", table: "wq", alias: "abc1" },
      withs: [{ query: withQuery, alias: "wq" }]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(
      compiled.sql,
      'with "a_wq" as (select ? as "q" from XYZ as "a_xyz1") select a_abc1."q" as "x" from a_wq as "a_abc1"'
    )
    assert.deepEqual(compiled.params, [5])
  })

  it("compiles union all query", async function () {
    const query1: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const query2: JsonQLQuery = {
      type: "query",
      selects: [{ type: "select", expr: { type: "literal", value: 5 }, alias: "x" }],
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    const query: JsonQLQuery = {
      type: "union all",
      queries: [query1, query2]
    }

    const compiled = await compiler.compileQuery(query)
    assert.equal(
      compiled.sql,
      '(select ? as "x" from ABC as "a_abc1") union all (select ? as "x" from ABC as "a_abc1")'
    )
    assert.deepEqual(compiled.params, [4, 5])
  })

  it("compiles reused alias", async function () {
    const expr: JsonQLExpr = {
      type: "scalar",
      expr: { type: "field", tableAlias: "j1", column: "_id" },
      from: { type: "table", table: "entities.water_point", alias: "j1" },
      where: {
        type: "scalar",
        expr: { type: "field", tableAlias: "j1", column: "_id" },
        from: { type: "table", table: "entities.water_point", alias: "j1" },
        where: {
          type: "op",
          op: "=",
          exprs: [
            {
              type: "op",
              op: "coalesce",
              exprs: [
                {
                  type: "op",
                  op: "#>>",
                  exprs: [
                    { type: "field", tableAlias: "main", column: "data" },
                    "{e75938878a034797a08969f847629931,value,code}"
                  ]
                },
                {
                  type: "op",
                  op: "#>>",
                  exprs: [
                    { type: "field", tableAlias: "main", column: "data" },
                    "{e75938878a034797a08969f847629931,value}"
                  ]
                }
              ]
            },
            { type: "field", tableAlias: "j1", column: "code" }
          ]
        }
      }
    }
    const compiled = await compiler.compileExpr(expr, { main: "xyz" })
    return console.log(compiled.toInline())
  })

  // Not longer check this as subtables can have indeterminate columns
  // it 'check that with columns exist', ->
  //   withQuery = {
  //     type: "query"
  //     selects: [
  //       { type: "select", expr: { type: "literal", value: 5 }, alias: "q" }
  //     ]
  //     from: { type: "table", table: "xyz", alias: "xyz1" }
  //   }

  //   query = {
  //     type: "query"
  //     selects: [
  //       { type: "select", expr: { type: "field", tableAlias: "abc1", column: "xyzzy" }, alias: "x" }
  //     ]
  //     from: { type: "table", table: "wq", alias: "abc1" }
  //     withs: [
  //       { query: withQuery, alias: "wq" }
  //     ]
  //   }

  //   assert.throws () =>
  //     compiled = @compiler.compileQuery(query)

  it("validates select aliases", async function () {
    let failed = false
    try {
      const select = { expr: { type: "literal", value: 4 }, alias: "???" }
      await compiler.compileSelect(select, { test: "test" })
      failed = true
    } catch (err) {}
    if (failed) {
      assert.fail()
    }
  })

  it("validates aliases", function () {
    assert.throws(() => compiler.validateAlias(" 1234"))
    assert.throws(() => compiler.validateAlias("ab;c"))
    return compiler.validateAlias("abc")
  })

  it("compiles select with function", async function () {
    const select = { expr: { type: "op", op: "row_number", exprs: [] }, alias: "abc" }
    const compiled = await compiler.compileSelect(select, {})
    assert.equal(compiled.sql, 'row_number() as "abc"')
  })

  it("compiles select over with partitionBy (legacy)", async function () {
    const over = { partitionBy: [{ type: "field", tableAlias: "abc", column: "x" }] }
    const select = { expr: { type: "op", op: "row_number", exprs: [] }, over, alias: "xyz" }
    const sql = await compiler.compileSelect(select, { abc: "def" })
    assert.equal(sql.sql, '(row_number() over (partition by a_abc.X)) as "xyz"')
  })

  it("compiles select over with orderBy (legacy)", async function () {
    const over = { orderBy: [{ expr: { type: "field", tableAlias: "abc", column: "x" }, direction: "asc" }] }
    const select = { expr: { type: "op", op: "row_number", exprs: [] }, over, alias: "xyz" }
    const sql = await compiler.compileSelect(select, { abc: "def" })
    assert.equal(sql.sql, '(row_number() over ( order by a_abc.X asc)) as "xyz"')
  })

  it("compiles select over (legacy)", async function () {
    const over = {}
    const select = { expr: { type: "op", op: "row_number", exprs: [] }, over, alias: "xyz" }
    const sql = await compiler.compileSelect(select, { abc: "def" })
    assert.equal(sql.sql, '(row_number() over ()) as "xyz"')
  })

  describe("compiles froms", async function () {
    it("compiles table", async function () {
      const aliases = {}
      const result = await compiler.compileFrom({ type: "table", table: "abc", alias: "abc1" }, aliases)
      assert.equal(result.sql, 'ABC as "a_abc1"')
      assert.deepEqual(result.params, [])

      // Maps alias to table
      assert.deepEqual(aliases, { abc1: "abc" })
    })

    it("compiles join", async function () {
      const aliases = {}
      const result = await compiler.compileFrom(
        {
          type: "join",
          left: { type: "table", table: "abc", alias: "abc1" },
          right: { type: "table", table: "def", alias: "def1" },
          kind: "inner",
          on: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "abc1", column: "p" },
              { type: "field", tableAlias: "def1", column: "q" }
            ]
          }
        },
        aliases
      )
      assert.equal(result.sql, '(ABC as "a_abc1" inner join DEF as "a_def1" on (a_abc1.P = a_def1.Q))')
      assert.deepEqual(result.params, [])

      // Maps alias to table
      assert.deepEqual(aliases, { abc1: "abc", def1: "def" })
    })

    it("compiles cross join", async function () {
      const aliases = {}
      const result = await compiler.compileFrom(
        {
          type: "join",
          left: { type: "table", table: "abc", alias: "abc1" },
          right: { type: "table", table: "def", alias: "def1" },
          kind: "cross"
        },
        aliases
      )
      assert.equal(result.sql, '(ABC as "a_abc1" cross join DEF as "a_def1")')
      assert.deepEqual(result.params, [])
    })

    it("prevents duplicate aliases", async function () {
      let failed = false
      try {
        await compiler.compileFrom(
          {
            type: "join",
            left: { type: "table", table: "abc", alias: "abc1" },
            right: { type: "table", table: "def", alias: "abc1" },
            kind: "inner",
            on: {
              type: "op",
              op: "=",
              exprs: [
                { type: "field", tableAlias: "abc1", column: "p" },
                { type: "field", tableAlias: "def1", column: "q" }
              ]
            }
          },
          {}
        )
        failed = true
      } catch (err) {}
      if (failed) {
        assert.fail()
      }
    })

    it("validates kind", async function () {
      let failed = false
      try {
        await compiler.compileFrom({
          type: "join",
          left: { type: "table", table: "abc", alias: "abc1" },
          right: { type: "table", table: "def", alias: "def1" },
          kind: "xyz",
          on: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "abc1", column: "p" },
              { type: "field", tableAlias: "def1", column: "q" }
            ]
          }
        })
        failed = true
      } catch (err) {}
      if (failed) {
        assert.fail()
      }
    })
  })

  describe("compiles expressions", async function () {
    before(function () {
      this.a = { type: "literal", value: 1 }
      this.b = { type: "literal", value: 2 }
      this.c = { type: "literal", value: 3 }
      this.d = { type: "literal", value: 4 }
      this.e = { type: "literal", value: 5 }
      this.str = { type: "literal", value: "xyz" }

      this.testExpr = async function (expr: any, sql: any, params: any, aliases = {}) {
        const fr = await compiler.compileExpr(expr, aliases)
        assert.equal(fr.sql, sql)
        assert.deepEqual(fr.params, params)
      };
    })

    it("literal", async function () {
      this.testExpr({ type: "literal", value: "abc" }, "?", ["abc"])
    })

    it("JSON literals", async function () {
      this.testExpr("abc", "?", ["abc"])
      this.testExpr(2.3, "?", [2.3])
      this.testExpr(true, "?", [true])
      this.testExpr(false, "?", [false])
    })

    it("null", async function () {
      this.testExpr(null, "null", [])
    })

    it("token", async function () {
      this.testExpr({ type: "token", token: "!bbox!" }, "!bbox!", [])
      // assert.throws(() => this.testExpr({ type: "token", token: "bbox" }, "!bbox!", []))
    })

    describe("case", async function () {
      it("does input case", async function () {
        this.testExpr(
          { type: "case", input: this.a, cases: [{ when: this.b, then: this.c }] },
          "case ? when ? then ? end",
          [1, 2, 3]
        )
      })

      it("does multiple case with else", async function () {
        this.testExpr(
          {
            type: "case",
            cases: [
              { when: this.a, then: this.b },
              { when: this.c, then: this.d }
            ],
            else: this.e
          },
          "case when ? then ? when ? then ? else ? end",
          [1, 2, 3, 4, 5]
        )
      })
    })

    describe("ops", async function () {
      it("> < >= <= = <>", async function () {
        this.testExpr({ type: "op", op: ">", exprs: [this.a, this.b] }, "(? > ?)", [1, 2])
        this.testExpr({ type: "op", op: "<", exprs: [this.a, this.b] }, "(? < ?)", [1, 2])
        this.testExpr({ type: "op", op: ">=", exprs: [this.a, this.b] }, "(? >= ?)", [1, 2])
        this.testExpr({ type: "op", op: "<=", exprs: [this.a, this.b] }, "(? <= ?)", [1, 2])
        this.testExpr({ type: "op", op: "=", exprs: [this.a, this.b] }, "(? = ?)", [1, 2])
        this.testExpr({ type: "op", op: "<>", exprs: [this.a, this.b] }, "(? <> ?)", [1, 2])
      })

      it("and", async function () {
        this.testExpr({ type: "op", op: "and", exprs: [] }, "", [])
        this.testExpr({ type: "op", op: "and", exprs: [this.a] }, "?", [1])
        this.testExpr({ type: "op", op: "and", exprs: [this.a, this.b, this.c] }, "(? and ? and ?)", [1, 2, 3])
        this.testExpr(
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: "and", exprs: [] },
              { type: "op", op: "and", exprs: [this.a] }
            ]
          },
          "?",
          [1]
        )
      })

      it("or", async function () {
        this.testExpr({ type: "op", op: "or", exprs: [] }, "", [])
        this.testExpr({ type: "op", op: "or", exprs: [this.a] }, "?", [1])
        this.testExpr({ type: "op", op: "or", exprs: [this.a, this.b, this.c] }, "(? or ? or ?)", [1, 2, 3])
      })

      it("not", async function () {
        this.testExpr({ type: "op", op: "not", exprs: [this.a] }, "(not ?)", [1])
      })

      it("is null", async function () {
        this.testExpr({ type: "op", op: "is null", exprs: [this.a] }, "(? is null)", [1])
      })

      it("is not null", async function () {
        this.testExpr({ type: "op", op: "is not null", exprs: [this.a] }, "(? is not null)", [1])
      })

      it("in", async function () {
        this.testExpr({ type: "op", op: "in", exprs: [this.a, this.b] }, "(? in ?)", [1, 2])
      })

      it("+ - *", async function () {
        this.testExpr({ type: "op", op: "+", exprs: [this.a, this.b] }, "(? + ?)", [1, 2])
        this.testExpr({ type: "op", op: "-", exprs: [this.a, this.b] }, "(? - ?)", [1, 2])
        this.testExpr({ type: "op", op: "*", exprs: [this.a, this.b] }, "(? * ?)", [1, 2])
        this.testExpr({ type: "op", op: "+", exprs: [this.a, this.b, this.c] }, "(? + ? + ?)", [1, 2, 3])
        this.testExpr({ type: "op", op: "-", exprs: [this.a, this.b, this.c] }, "(? - ? - ?)", [1, 2, 3])
        this.testExpr({ type: "op", op: "*", exprs: [this.a, this.b, this.c] }, "(? * ? * ?)", [1, 2, 3])
      })

      it("/", async function () {
        this.testExpr({ type: "op", op: "/", exprs: [this.a, this.b] }, "(? / ?)", [1, 2])
      })

      it("||", async function () {
        this.testExpr({ type: "op", op: "||", exprs: [this.a, this.b, this.c] }, "(? || ? || ?)", [1, 2, 3])
      })

      it("~ ~* like ilike", async function () {
        this.testExpr({ type: "op", op: "~", exprs: [this.a, this.b] }, "(? ~ ?)", [1, 2])
        this.testExpr({ type: "op", op: "~*", exprs: [this.a, this.b] }, "(? ~* ?)", [1, 2])
        this.testExpr({ type: "op", op: "like", exprs: [this.a, this.b] }, "(? like ?)", [1, 2])
        this.testExpr({ type: "op", op: "ilike", exprs: [this.a, this.b] }, "(? ilike ?)", [1, 2])
      })

      it("::text", async function () {
        this.testExpr({ type: "op", op: "::text", exprs: [this.a] }, "(?::text)", [1])
      })

      it("[]", async function () {
        this.testExpr({ type: "op", op: "[]", exprs: [this.a, this.b] }, "((?)[?])", [1, 2])
      })

      it("= any", async function () {
        const arr = { type: "literal", value: ["x", "y"] }
        this.testExpr({ type: "op", op: "=", modifier: "any", exprs: [this.a, arr] }, "(? = any(?))", [
          1,
          ["x", "y"]
        ])
      })

      it("->> #>>", async function () {
        this.testExpr({ type: "op", op: "->>", exprs: [this.a, this.b] }, "(? ->> ?)", [1, 2])
        this.testExpr({ type: "op", op: "#>>", exprs: [this.a, this.b] }, "(? #>> ?)", [1, 2])
      })

      it("between", async function () {
        this.testExpr(
          { type: "op", op: "between", exprs: [this.a, this.b, this.c] },
          "(? between ? and ?)",
          [1, 2, 3]
        )
      })

      it("aggregate expressions", async function () {
        this.testExpr({ type: "op", op: "avg", exprs: [this.a] }, "avg(?)", [1])
        this.testExpr({ type: "op", op: "min", exprs: [this.a] }, "min(?)", [1])
        this.testExpr({ type: "op", op: "max", exprs: [this.a] }, "max(?)", [1])
        this.testExpr({ type: "op", op: "sum", exprs: [this.a] }, "sum(?)", [1])
        this.testExpr({ type: "op", op: "count", exprs: [this.a] }, "count(?)", [1])
        this.testExpr({ type: "op", op: "stdev", exprs: [this.a] }, "stdev(?)", [1])
        this.testExpr({ type: "op", op: "stdevp", exprs: [this.a] }, "stdevp(?)", [1])
        this.testExpr({ type: "op", op: "var", exprs: [this.a] }, "var(?)", [1])
        this.testExpr({ type: "op", op: "varp", exprs: [this.a] }, "varp(?)", [1])
        this.testExpr({ type: "op", op: "count", exprs: [] }, "count(*)", [])
        this.testExpr({ type: "op", op: "count", modifier: "distinct", exprs: [this.a] }, "count(distinct ?)", [1])
        this.testExpr({ type: "op", op: "unnest", exprs: [this.a] }, "unnest(?)", [1])
        let failed = false
        try {
          await compiler.compileExpr({ type: "op", op: "xyz", exprs: [this.a] }, {})
          failed = true
        } catch (err) {}
        if (failed) {
          assert.fail()
        }
      })

      it("array_agg with orderBy", async function () {
        const orderBy = [{ expr: { type: "field", tableAlias: "abc", column: "x" }, direction: "asc" }]
        const expr = { type: "op", op: "array_agg", exprs: [this.a], orderBy }
        this.testExpr(expr, "array_agg(? order by a_abc.X asc)", [1], { abc: "abc" })
      })

      it("aggregate over with partitionBy", async function () {
        const over = { partitionBy: [{ type: "field", tableAlias: "abc", column: "x" }] }
        const expr = { type: "op", op: "row_number", exprs: [], over }
        this.testExpr(expr, "(row_number() over (partition by a_abc.X))", [], { abc: "def" })
      })

      it("aggregate over with orderBy", async function () {
        const over = { orderBy: [{ expr: { type: "field", tableAlias: "abc", column: "x" }, direction: "asc" }] }
        const expr = { type: "op", op: "row_number", exprs: [], over }
        this.testExpr(expr, "(row_number() over ( order by a_abc.X asc))", [], { abc: "def" })
      })

      it("aggregate over", async function () {
        const over = {}
        const expr = { type: "op", op: "row_number", exprs: [], over }
        this.testExpr(expr, "(row_number() over ())", [])
      })

      it("creates exists", async function () {
        const query: JsonQLQuery = {
          type: "query",
          selects: [{ type: "select", expr: { type: "literal", value: 4 }, alias: "x" }],
          from: { type: "table", table: "abc", alias: "abc1" }
        }

        this.testExpr(
          { type: "op", op: "exists", exprs: [query] },
          'exists (select ? as "x" from ABC as "a_abc1")',
          [4]
        )
      })

      it("interval", async function () {
        this.testExpr({ type: "op", op: "interval", exprs: [this.str] }, "(interval ?)", ["xyz"])
      })

      it("at time zome", async function () {
        this.testExpr(
          { type: "op", op: "at time zone", exprs: [{ type: "op", op: "now", exprs: [] }, this.str] },
          "(now() at time zone ?)",
          ["xyz"]
        )
      })
    })

    return describe("scalar", async function () {
      it("simple scalar", async function () {
        this.testExpr(
          { type: "scalar", expr: this.a, from: { type: "table", table: "abc", alias: "abc1" } },
          '(select ? from ABC as "a_abc1")',
          [1]
        )
      })

      it("scalar with orderBy expr", async function () {
        this.testExpr(
          {
            type: "scalar",
            expr: this.a,
            from: { type: "table", table: "abc", alias: "abc1" },
            orderBy: [
              {
                expr: this.b,
                direction: "desc"
              }
            ]
          },
          '(select ? from ABC as "a_abc1" order by ? desc)',
          [1, 2]
        )
      })

      it("compiles scalar with withs", async function () {
        const withQuery = {
          type: "query",
          selects: [{ type: "select", expr: { type: "literal", value: 5 }, alias: "q" }],
          from: { type: "table", table: "xyz", alias: "xyz1" }
        }

        this.testExpr(
          {
            type: "scalar",
            expr: this.a,
            from: { type: "table", table: "wq", alias: "abc1" },
            withs: [{ query: withQuery, alias: "wq" }],
            orderBy: [
              {
                expr: this.b,
                direction: "desc"
              }
            ]
          },
          '(with "a_wq" as (select ? as "q" from XYZ as "a_xyz1") select ? from a_wq as "a_abc1" order by ? desc)',
          [5, 1, 2]
        )
      })
    })
  });
})
