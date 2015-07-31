assert = require('chai').assert
SqlFragment = require '../src/SqlFragment'
SchemaMap = require '../src/SchemaMap'
JsonqlCompiler = require '../src/JsonqlCompiler'

# Capitalizes tables and columns and aliases
class MockSchemaMap extends SchemaMap
  mapTable: (table) -> 
    return new SqlFragment(table.toUpperCase())

  # Map a column reference of a table aliased as alias
  mapColumn: (table, column, alias) ->
    return new SqlFragment(alias + "." + column.toUpperCase())

  mapTableAlias: (alias) ->
    return "a_" + alias

describe "JsonqlCompiler", ->
  beforeEach ->
    @compiler = new JsonqlCompiler(new MockSchemaMap())

  it 'compiles simple query', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 4 }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select ? as "x" from ABC as "a_abc1"'
    assert.deepEqual compiled.params, [4]

  it 'compiles query with field', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.P as "x" from ABC as "a_abc1"'
    assert.deepEqual compiled.params, []

  it 'compiles query with where', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      where: { type: "op", op: ">", exprs: [
        { type: "field", tableAlias: "abc1", column: "q" }
        { type: "literal", value: 5 }
        ]}
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.P as "x" from ABC as "a_abc1" where (a_abc1.Q > ?)'
    assert.deepEqual compiled.params, [5]

  it 'compiles query with groupBy', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      groupBy: [1, 2]
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" group by 1, 2'
    assert.deepEqual compiled.params, []

  it 'compiles query with orderBy ordinal', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      orderBy: [
        { ordinal: 1, direction: "desc" }
        { ordinal: 2 }
      ]
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" order by 1 desc, 2'
    assert.deepEqual compiled.params, []

  it 'compiles query with orderBy expr', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "y" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      orderBy: [
        { expr: { type: "field", tableAlias: "abc1", column: "q" } }
      ]
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.P as "x", a_abc1.Q as "y" from ABC as "a_abc1" order by a_abc1.Q'
    assert.deepEqual compiled.params, []

  it 'compiles query with limit', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 4 }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      limit: 10
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select ? as "x" from ABC as "a_abc1" limit ?'
    assert.deepEqual compiled.params, [4, 10]

  it 'compiles query with offset', ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 4 }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
      offset: 10
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select ? as "x" from ABC as "a_abc1" offset ?'
    assert.deepEqual compiled.params, [4, 10]

  it 'compiles query with subqueries', ->
    subquery = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 5 }, alias: "q" }
      ]
      from: { type: "table", table: "xyz", alias: "xyz1" }
    }

    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "x" }
      ]
      from: { type: "subquery", query: subquery, alias: "abc1" }
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'select a_abc1.q as "x" from (select ? as "q" from XYZ as "a_xyz1") as "a_abc1"'
    assert.deepEqual compiled.params, [5]

  it 'compiles query with withs', ->
    withQuery = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 5 }, alias: "q" }
      ]
      from: { type: "table", table: "xyz", alias: "xyz1" }
    }

    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "q" }, alias: "x" }
      ]
      from: { type: "table", table: "wq", alias: "abc1" }
      withs: [
        { query: withQuery, alias: "wq" }
      ]
    }

    compiled = @compiler.compileQuery(query)
    assert.equal compiled.sql, 'with "a_wq" as (select ? as "q" from XYZ as "a_xyz1") select a_abc1.q as "x" from a_wq as "a_abc1"'
    assert.deepEqual compiled.params, [5]

  it 'check that with columns exist', ->
    withQuery = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "literal", value: 5 }, alias: "q" }
      ]
      from: { type: "table", table: "xyz", alias: "xyz1" }
    }

    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "abc1", column: "xyzzy" }, alias: "x" }
      ]
      from: { type: "table", table: "wq", alias: "abc1" }
      withs: [
        { query: withQuery, alias: "wq" }
      ]
    }

    assert.throws () =>
      compiled = @compiler.compileQuery(query)

  it 'validates select aliases', ->
    assert.throws () =>
      select = { expr: { type: "literal", value: 4 }, alias: "???" }
      @compiler.compileSelect(select, { test: "test" })

  it 'validates aliases', ->
    assert.throws () => @compiler.validateAlias("1234")
    assert.throws () => @compiler.validateAlias("ab c")
    assert.throws () => @compiler.validateAlias("ab'c")
    @compiler.validateAlias("abc")

  it 'compiles select with function', ->
    select = { expr: { type: "op", op: "row_number", exprs: [] }, alias: "abc" }
    assert.equal @compiler.compileSelect(select, {}).sql, "row_number() as \"abc\""

  it 'compiles select over with partitionBy', ->
    over = { partitionBy: [{ type: "field", tableAlias: "abc", column: "x" }] }
    select = { expr: { type: "op", op: "row_number", exprs: [] }, over: over, alias: "xyz" }
    sql = @compiler.compileSelect(select, { abc: "def" })
    assert.equal sql.sql, "row_number() over (partition by a_abc.X) as \"xyz\""

  describe "compiles froms", ->
    it 'compiles table', ->
      aliases = {}
      result = @compiler.compileFrom({ type: "table", table: "abc", alias: "abc1" }, aliases)
      assert.equal result.sql, 'ABC as "a_abc1"'
      assert.deepEqual result.params, []

      # Maps alias to table
      assert.deepEqual aliases, { "abc1": "abc" }

    it 'compiles join', ->
      aliases = {}
      result = @compiler.compileFrom({
        type: "join"
        left: { type: "table", table: "abc", alias: "abc1" }
        right: { type: "table", table: "def", alias: "def1" }
        kind: "inner"
        on: { type: "op", op: "=", exprs: [
          { type: "field", tableAlias: "abc1", column: "p" }
          { type: "field", tableAlias: "def1", column: "q" }
        ]}
      }, aliases)
      assert.equal result.sql, '(ABC as "a_abc1" inner join DEF as "a_def1" on (a_abc1.P = a_def1.Q))'
      assert.deepEqual result.params, []
      
      # Maps alias to table
      assert.deepEqual aliases, { "abc1": "abc", "def1": "def" }

    it 'prevents duplicate aliases', ->
      assert.throws () =>
        @compiler.compileFrom({
          type: "join"
          left: { type: "table", table: "abc", alias: "abc1" }
          right: { type: "table", table: "def", alias: "abc1" }
          kind: "inner"
          on: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "abc1", column: "p" }
            { type: "field", tableAlias: "def1", column: "q" }
          ]}
        }, {})

    it 'validates kind', ->
      assert.throws () =>
        @compiler.compileFrom({
          type: "join"
          left: { type: "table", table: "abc", alias: "abc1" }
          right: { type: "table", table: "def", alias: "def1" }
          kind: "xyz"
          on: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "abc1", column: "p" }
            { type: "field", tableAlias: "def1", column: "q" }
          ]}
        })

  describe "compiles expressions", ->
    before ->
      @a = { type: "literal", value: 1 }
      @b = { type: "literal", value: 2 }
      @c = { type: "literal", value: 3 }
      @d = { type: "literal", value: 4 }
      @e = { type: "literal", value: 5 }

      @testExpr = (expr, sql, params, aliases={}) ->
        fr = @compiler.compileExpr(expr, aliases)
        assert.equal fr.sql, sql
        assert.deepEqual fr.params, params

    it 'literal', ->
      @testExpr({ type: "literal", value: "abc" }, "?", ["abc"])

    it 'JSON literals', ->
      @testExpr("abc", "?", ["abc"])
      @testExpr(2.3, "?", [2.3])
      @testExpr(true, "?", [true])
      @testExpr(false, "?", [false])

    it 'null', ->
      @testExpr(null, "null", [])

    it 'token', ->
      @testExpr({ type: "token", token: "!bbox!"}, "!bbox!", [])
      assert.throws () => @testExpr({ type: "token", token: "bbox"}, "!bbox!", [])

    describe "case", ->
      it "does input case", ->
        @testExpr({ type: "case", input: @a, cases: [{ when: @b, then: @c }]}, 
          "case ? when ? then ? end", [1, 2, 3]
          )
      
      it "does multiple case with else", ->
        @testExpr({ type: "case", cases: [
          { when: @a, then: @b }
          { when: @c, then: @d }
          ], else: @e }, 
          "case when ? then ? when ? then ? else ? end", [1, 2, 3, 4, 5]
        )
      
    describe "ops", ->
      it '> < >= <= = <>', ->
        @testExpr({ type: "op", op: ">", exprs: [@a, @b] }, "(? > ?)", [1, 2])
        @testExpr({ type: "op", op: "<", exprs: [@a, @b] }, "(? < ?)", [1, 2])
        @testExpr({ type: "op", op: ">=", exprs: [@a, @b] }, "(? >= ?)", [1, 2])
        @testExpr({ type: "op", op: "<=", exprs: [@a, @b] }, "(? <= ?)", [1, 2])
        @testExpr({ type: "op", op: "=", exprs: [@a, @b] }, "(? = ?)", [1, 2])
        @testExpr({ type: "op", op: "<>", exprs: [@a, @b] }, "(? <> ?)", [1, 2])

      it 'and', ->
        @testExpr({ type: "op", op: "and", exprs: [] }, "", [])
        @testExpr({ type: "op", op: "and", exprs: [@a] }, "?", [1])
        @testExpr({ type: "op", op: "and", exprs: [@a, @b, @c] }, "(? and ? and ?)", [1, 2, 3])

      it 'or', ->
        @testExpr({ type: "op", op: "or", exprs: [] }, "", [])
        @testExpr({ type: "op", op: "or", exprs: [@a] }, "?", [1])
        @testExpr({ type: "op", op: "or", exprs: [@a, @b, @c] }, "(? or ? or ?)", [1, 2, 3])

      it 'not', ->
        @testExpr({ type: "op", op: "not", exprs: [@a] }, "(not ?)", [1])

      it 'is null', ->
        @testExpr({ type: "op", op: "is null", exprs: [@a] }, "(? is null)", [1])

      it 'is not null', ->
        @testExpr({ type: "op", op: "is not null", exprs: [@a] }, "(? is not null)", [1])
  
      it '+ - * /', ->
        @testExpr({ type: "op", op: "+", exprs: [@a, @b] }, "(? + ?)", [1, 2])
        @testExpr({ type: "op", op: "-", exprs: [@a, @b] }, "(? - ?)", [1, 2])
        @testExpr({ type: "op", op: "*", exprs: [@a, @b] }, "(? * ?)", [1, 2])
        @testExpr({ type: "op", op: "/", exprs: [@a, @b] }, "(? / ?)", [1, 2])

      it '~ ~* like', ->
        @testExpr({ type: "op", op: "~", exprs: [@a, @b] }, "(? ~ ?)", [1, 2])
        @testExpr({ type: "op", op: "~*", exprs: [@a, @b] }, "(? ~* ?)", [1, 2])
        @testExpr({ type: "op", op: "like", exprs: [@a, @b] }, "(? like ?)", [1, 2])

      it '::text', ->
        @testExpr({ type: "op", op: "::text", exprs: [@a] }, "(?::text)", [1])   

      it '= any', ->
        arr = { type: "literal", value: ["x", "y"] }
        @testExpr({ type: "op", op: "=", modifier: "any", exprs: [@a, arr] }, "(? = any(?))", [1, ["x", "y"]])

      it '->> #>>', ->
        @testExpr({ type: "op", op: "->>", exprs: [@a, @b] }, "(? ->> ?)", [1, 2])
        @testExpr({ type: "op", op: "#>>", exprs: [@a, @b] }, "(? #>> ?)", [1, 2])

      it 'aggregate expressions', ->
        @testExpr({ type: "op", op: "avg", exprs: [@a] }, "avg(?)", [1])
        @testExpr({ type: "op", op: "min", exprs: [@a] }, "min(?)", [1])
        @testExpr({ type: "op", op: "max", exprs: [@a] }, "max(?)", [1])
        @testExpr({ type: "op", op: "sum", exprs: [@a] }, "sum(?)", [1])
        @testExpr({ type: "op", op: "count", exprs: [@a] }, "count(?)", [1])
        @testExpr({ type: "op", op: "stdev", exprs: [@a] }, "stdev(?)", [1])
        @testExpr({ type: "op", op: "stdevp", exprs: [@a] }, "stdevp(?)", [1])
        @testExpr({ type: "op", op: "var", exprs: [@a] }, "var(?)", [1])
        @testExpr({ type: "op", op: "varp", exprs: [@a] }, "varp(?)", [1])
        @testExpr({ type: "op", op: "count", exprs: [] }, "count(*)", [])
        assert.throws () =>
          @testExpr({ type: "op", op: "xyz", exprs: [@a] }, "xyz(?)", [1])

    describe "scalar", ->
      it "simple scalar", ->
        @testExpr({ type: "scalar", expr: @a, from: { type: "table", table: "abc", alias: "abc1" } }, '(select ? from ABC as "a_abc1")', [1])
      it "scalar with orderBy expr", ->
        @testExpr({ 
          type: "scalar"
          expr: @a
          from: { type: "table", table: "abc", alias: "abc1" } 
          orderBy: [{
            expr: @b
            direction: "desc"
            }]
        }, '(select ? from ABC as "a_abc1" order by ? desc)', [1,2]) 

