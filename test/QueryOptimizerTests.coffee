assert = require('chai').assert
QueryOptimizer = require '../src/QueryOptimizer'
canonical = require 'canonical-json'

compare = (actual, expected) ->
  strActual = canonical(actual)
  strExpected = canonical(expected)
  if strActual != strExpected
    for i in [0...Math.min(strActual.length, strExpected.length)]
      if strActual[i] != strExpected[i]
        console.log "got: " + strActual.substr(Math.max(i - 20, 0), 80)
        console.log "exp: " + strExpected.substr(Math.max(i - 20, 0), 80)
        break
  assert.equal canonical(actual), canonical(expected), "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n"

describe "QueryOptimizer", ->
  beforeEach ->
    @opt = new QueryOptimizer()

  it 'gets fields', ->
    expr = { 
      type: "op"
      op: "="
      exprs: [
        { type: "field", tableAlias: "a", column: "a1" }
        { type: "literal", value: 3 }
      ]
    }

    compare @opt.extractFields(expr), [{ type: "field", tableAlias: "a", column: "a1" }]

  it "determines if expr is aggregate", ->
    assert.isFalse @opt.isAggr({ type: "field", tableAlias: "a", column: "a1" })
    assert.isFalse @opt.isAggr({ type: "op", op: "+", exprs: [{ type: "field", tableAlias: "a", column: "a1" }, 2] })
    assert.isTrue @opt.isAggr({ type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "a", column: "a1" }] })

  it "extracts from aliases", ->
    table = { type: "join", kind: "left", left: { type: "table", table: "a", alias: "A" }, right: { type: "table", table: "b", alias: "B" } }
    assert.deepEqual @opt.extractFromAliases(table), ["A", "B"]

  it "remaps case statements"
  it "remaps ops"

  it 'optimizes simple non-aggr select', ->
    ###
    select a.a1 as s1, (select b.b1 from b as b where b.b2 = a.a2) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt0.opt_a_a1 as s1, opt0.expr as s2
    from
    ****** First query opt0 that does left outer join
    (
      select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, b.b1 as expr from a as a
      left outer join b as b on b.b2 = a.a2
    ) as opt0
    order by opt0.opt_a_a3

    ###
    input = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" }
        { 
          type: "select"
          expr: { 
            type: "scalar"
            expr: { type: "field", tableAlias: "b", column: "b1" }
            from: { type: "table", table: "b", alias: "b" }
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          }
          alias: "s2" 
        }
      ]
      from: { type: "table", table: "a", alias: "a" }
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    }

    output = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a1" }, alias: "s1" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "expr" }, alias: "s2" }
      ]
      from: {
        type: "subquery"
        query: {
          type: "query"
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" }
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" }
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" }
            { type: "select", expr: { type: "field", tableAlias: "b", column: "b1" }, alias: "expr" }
          ]
          from: { 
            type: "join"
            kind: "left"
            left: { type: "table", table: "a", alias: "a" }
            right: { type: "table", table: "b", alias: "b" }
            on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          }
          where: null
        }
        alias: "opt0"
      }
      where: null
      orderBy: [{ expr: { type: "field", tableAlias: "opt0", column: "opt_a_a3" }}]
    }

    compare @opt.rewriteScalar(input), output

  it 'optimizes simple aggr select', ->
    ###
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt1.opt_a_a1 as s1, opt1.expr as s2
    from
    ****** Second query opt1 that does left outer join 
    (
      select opt0.rn as rn, opt0.opt_a_a1 as opt_a_a1, opt0.opt_a_a2 as opt_a_a2, opt0.opt_a_a3 as opt_a_a3, sum(b.b1) as expr
      from 
      ****** First query opt0 that adds row number 
      (
        select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, row_number() over () as rn from a as a
      ) as opt0
      left outer join b as b on b.b2 = opt0.opt_a_a2
      ****** group by all fields except expr 
      group by 1, 2, 3, 4
    ) as opt1
    order by opt1.opt_a_a3

    ###
    input = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" }
        { 
          type: "select"
          expr: { 
            type: "scalar"
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] }
            from: { type: "table", table: "b", alias: "b" }
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          }
          alias: "s2" 
        }
      ]
      from: { type: "table", table: "a", alias: "a" }
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    }

    inner0Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ]
      from: { type: "table", table: "a", alias: "a" }
      where: null
    }

    output = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "s1" }
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "expr" }, alias: "s2" }
      ]
      from: {
        type: "subquery"
        query: {
          type: "query"
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "opt0", column: "rn" }, alias: "rn" }
            { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a1" }, alias: "opt_a_a1" }
            { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a2" }, alias: "opt_a_a2" }
            { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a3" }, alias: "opt_a_a3" }
            { type: "select", expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] }, alias: "expr" }
          ]
          from: { 
            type: "join"
            kind: "left"
            left: { type: "subquery", query: inner0Query, alias: "opt0" }
            right: { type: "table", table: "b", alias: "b" }
            on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "opt0", column: "opt_a_a2" }] }
          }
          groupBy: [1, 2, 3, 4]
        }
        alias: "opt1"
      }
      where: null
      orderBy: [{ expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }}]
    }

    # console.log JSON.stringify(output, null, 2)
    compare @opt.rewriteScalar(input), output

  it 'optimizes simple limit select', ->
    ###
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2 order by b.b3 limit 1) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt2.opt_a_a1 as s1, opt2.expr as s2
    from
    ****** Third query opt2 that removes all but top row of window 
    (
      select opt1.opt_a_a1 as opt_a_a1, opt1.opt_a_a2 as opt_a_a2, opt1.opt_a_a3 as opt_a_a3, expr as expr
      from 
      ****** Second query opt1 that does left outer join and adds row number 
      (
        select opt0.opt_a_a1 as opt_a_a1, opt0.opt_a_a2 as opt_a_a2, opt0.opt_a_a3 as opt_a_a3, b.b1 as expr, row_number() over (partition by opt0.rn order by b.b3) as rn
        from 
        ****** First query opt0 that adds row number 
        (
          select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, row_number() over () as rn from a as a
        ) as opt0
        left outer join b as b on b.b2 = opt0.opt_a_a2
      ) as opt1
      where opt1.rn = 1
    ) as opt2
    order by opt2.opt_a_a3

    ###
    input = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" }
        { 
          type: "select"
          expr: { 
            type: "scalar"
            expr: { type: "field", tableAlias: "b", column: "b1" }
            from: { type: "table", table: "b", alias: "b" }
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
            orderBy: [{ expr: { type: "field", tableAlias: "b", column: "b3" }, direction: "asc" }]
            limit: 1
          }
          alias: "s2" 
        }
      ]
      from: { type: "table", table: "a", alias: "a" }
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    }

    inner0Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ]
      from: { type: "table", table: "a", alias: "a" }
      where: null
    }
    
    inner1Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "field", tableAlias: "b", column: "b1" }, alias: "expr" }
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: { 
          partitionBy: [{ type: "field", tableAlias: "opt0", column: "rn" }]
          orderBy: [{ expr: { type: "field", tableAlias: "b", column: "b3" }, direction: "asc" }]
        }, alias: "rn" }
      ]
      from: { 
        type: "join"
        kind: "left"
        left: { type: "subquery", query: inner0Query, alias: "opt0" }
        right: { type: "table", table: "b", alias: "b" }
        on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "opt0", column: "opt_a_a2" }] }
      }
    }

    inner2Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "expr" }, alias: "expr" }
      ]
      from: { type: "subquery", query: inner1Query, alias: "opt1" }
      where: {
        type: "op"
        op: "="
        exprs: [
          { type: "field", tableAlias: "opt1", column: "rn" }
          { type: "literal", value: 1 }
        ]
      }
    }

    output = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a1" }, alias: "s1" }
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "expr" }, alias: "s2" }
      ]
      from: {
        type: "subquery"
        query: inner2Query
        alias: "opt2"
      }
      where: null
      orderBy: [{ expr: { type: "field", tableAlias: "opt2", column: "opt_a_a3" }}]
    }

    compare @opt.rewriteScalar(input), output

  it 'optimizes aggr select with where', ->
    ###
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2) as s2
    from a as a
    where (select sum(c.c1) from c as c where c.c2 = a.a4) = 2 and a.a5 = 3
    order by a.a3

    should be optimized to (after one pass): 
    
    select opt1.opt_a_a1 as s1, (select sum(b.b1) from b as b where b.b2 = opt1.a_a2) as s2
    from
    ****** Second query opt1 that does left outer join 
    (
      select opt0.rn as rn, opt0.opt_a_a1 as opt_a_a1, opt0.opt_a_a2 as opt_a_a2, opt0.opt_a_a3 as opt_a_a3, sum(c.c1) as expr
      from 
      ****** First query opt0 that adds row number and does eligible wheres
      (
        select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, a.a4 as opt_a_a4, row_number() over () as rn 
        from a as a
        where a.a5 = 3
      ) as opt0
      left outer join c as c on c.c2 = opt0.opt_a_a4
      ****** group by all fields except expr 
      group by 1, 2, 3, 4
    ) as opt1
    where opt1.expr = 2
    order by opt1.opt_a_a3

    ###
    input = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" }
        { 
          type: "select"
          expr: { 
            type: "scalar"
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] }
            from: { type: "table", table: "b", alias: "b" }
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          }
          alias: "s2" 
        }
      ]
      from: { type: "table", table: "a", alias: "a" }
      where: {
        type: "op"
        op: "and"
        exprs: [
          { type: "op", op: "=", exprs: [
            { 
              type: "scalar"
              expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "c", column: "c1" }] }
              from: { type: "table", table: "c", alias: "c" }
              where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "c", column: "c2" }, { type: "field", tableAlias: "a", column: "a4" }] }
            }
            { type: "literal", value: 2 }
          ]}
          { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "a", column: "a5" }, { type: "literal", value: 3 }] }
        ]
      }
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    }

    inner0Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a4" }, alias: "opt_a_a4" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a5" }, alias: "opt_a_a5" }
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ]
      from: { type: "table", table: "a", alias: "a" }
      where: {
        type: "op"
        op: "and"
        exprs: [
          { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "a", column: "a5" }, { type: "literal", value: 3 }] }
        ]
      }
    }

    inner1Query = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "rn" }, alias: "rn" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a1" }, alias: "opt_a_a1" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a2" }, alias: "opt_a_a2" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a4" }, alias: "opt_a_a4" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a5" }, alias: "opt_a_a5" }
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_a_a3" }, alias: "opt_a_a3" }
        { type: "select", expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "c", column: "c1" }] }, alias: "expr" }
      ]
      from: { 
        type: "join"
        kind: "left"
        left: { type: "subquery", query: inner0Query, alias: "opt0" }
        right: { type: "table", table: "c", alias: "c" }
        on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "c", column: "c2" }, { type: "field", tableAlias: "opt0", column: "opt_a_a4" }] }
      }
      groupBy: [1, 2, 3, 4, 5, 6]
    }

    output = {
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "s1" }
        { 
          type: "select"
          expr: { 
            type: "scalar"
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] }
            from: { type: "table", table: "b", alias: "b" }
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "opt1", column: "opt_a_a2" }] }
          }
          alias: "s2" 
        }
      ]
      from: {
        type: "subquery"
        query: inner1Query
        alias: "opt1"
      }
      where: {
        type: "op"
        op: "and"
        exprs: [
          { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "opt1", column: "expr" }
            { type: "literal", value: 2 }        
          ]}
        ]
      }
      orderBy: [{ expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }}]
    }

    # console.log JSON.stringify(output, null, 2)
    compare @opt.rewriteScalar(input), output

