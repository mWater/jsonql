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
    return alias.toUpperCase()

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
    assert.equal compiled.sql, 'select ? as "x" from ABC as "ABC1"'
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
    assert.equal compiled.sql, 'select ABC1.P as "x" from ABC as "ABC1"'
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
    assert.equal compiled.sql, 'select ABC1.P as "x" from ABC as "ABC1" where (ABC1.Q > ?)'
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
    assert.equal compiled.sql, 'select ABC1.P as "x", ABC1.Q as "y" from ABC as "ABC1" group by ?, ?'
    assert.deepEqual compiled.params, [1, 2]

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
    assert.equal compiled.sql, 'select ABC1.P as "x", ABC1.Q as "y" from ABC as "ABC1" order by ? desc, ?'
    assert.deepEqual compiled.params, [1, 2]

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
    assert.equal compiled.sql, 'select ABC1.P as "x", ABC1.Q as "y" from ABC as "ABC1" order by ABC1.Q'
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
    assert.equal compiled.sql, 'select ? as "x" from ABC as "ABC1" limit ?'
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
    assert.equal compiled.sql, 'select ? as "x" from ABC as "ABC1" offset ?'
    assert.deepEqual compiled.params, [4, 10]

  it 'validates select aliases', ->
    assert.throws () =>
      select = { expr: { type: "literal", value: 4 }, alias: "???" }
      @compiler.compileSelect(select, "test", "test")

  it 'validates aliases', ->
    assert.throws () => @compiler.validateAlias("1234")
    assert.throws () => @compiler.validateAlias("ab c")
    assert.throws () => @compiler.validateAlias("ab'c")
    @compiler.validateAlias("abc")

  describe "compiles froms", ->
    it 'compiles table', ->
      result = @compiler.compileFrom({ type: "table", table: "abc", alias: "abc1" })
      assert.equal result.sql.sql, 'ABC as "ABC1"'
      assert.deepEqual result.sql.params, []

      # Maps alias to table
      assert.deepEqual result.aliases, { "abc1": "abc" }

    it 'compiles join', ->
      result = @compiler.compileFrom({
        type: "join"
        left: { type: "table", table: "abc", alias: "abc1" }
        right: { type: "table", table: "def", alias: "def1" }
        kind: "inner"
        on: { type: "op", op: "=", exprs: [
          { type: "field", tableAlias: "abc1", column: "p" }
          { type: "field", tableAlias: "def1", column: "q" }
        ]}
      })
      assert.equal result.sql.sql, '(ABC as "ABC1" inner join DEF as "DEF1" on (ABC1.P = DEF1.Q))'
      assert.deepEqual result.sql.params, []
      
      # Maps alias to table
      assert.deepEqual result.aliases, { "abc1": "abc", "def1": "def" }

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
        })

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
        @testExpr({ type: "op", op: "=", modifier: "any", exprs: [@a, @b] }, "(? = any(?))", [1, 2])

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
        assert.throws () =>
          @testExpr({ type: "op", op: "xyz", exprs: [@a] }, "xyz(?)", [1])

    describe "scalar", ->
      it "simple scalar", ->
        @testExpr({ type: "scalar", expr: @a, from: { type: "table", table: "abc", alias: "abc1" } }, '(select ? from ABC as "ABC1")', [1])
      it "scalar with orderBy expr", ->
        @testExpr({ 
          type: "scalar"
          expr: @a
          from: { type: "table", table: "abc", alias: "abc1" } 
          orderBy: [{
            expr: @b
            direction: "desc"
            }]
        }, '(select ? from ABC as "ABC1" order by ? desc)', [1,2]) 