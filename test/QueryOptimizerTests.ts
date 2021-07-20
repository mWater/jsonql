// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from 'chai';
import QueryOptimizer from '../src/QueryOptimizer';
import canonical from 'canonical-json';

function compare(actual, expected) {
  const strActual = canonical(actual);
  const strExpected = canonical(expected);
  if (strActual !== strExpected) {
    for (let i = 0, end = Math.min(strActual.length, strExpected.length), asc = 0 <= end; asc ? i < end : i > end; asc ? i++ : i--) {
      if (strActual[i] !== strExpected[i]) {
        console.log("got: " + strActual.substr(Math.max(i - 20, 0), 80));
        console.log("exp: " + strExpected.substr(Math.max(i - 20, 0), 80));
        break;
      }
    }
  }
  return assert.equal(canonical(actual), canonical(expected), "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n");
}

describe("QueryOptimizer", function() {
  beforeEach(function() {
    return this.opt = new QueryOptimizer();
  });

  it('gets fields', function() {
    const expr = { 
      type: "op",
      op: "=",
      exprs: [
        { type: "field", tableAlias: "a", column: "a1" },
        { type: "literal", value: 3 }
      ]
    };

    return compare(this.opt.extractFields(expr), [{ type: "field", tableAlias: "a", column: "a1" }]);
});

  it("determines if expr is aggregate", function() {
    assert.isFalse(this.opt.isAggr({ type: "field", tableAlias: "a", column: "a1" }));
    assert.isFalse(this.opt.isAggr({ type: "op", op: "+", exprs: [{ type: "field", tableAlias: "a", column: "a1" }, 2] }));
    return assert.isTrue(this.opt.isAggr({ type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "a", column: "a1" }] }));
  });

  it("extracts from aliases", function() {
    const table = { type: "join", kind: "left", left: { type: "table", table: "a", alias: "A" }, right: { type: "table", table: "b", alias: "B" } };
    return assert.deepEqual(this.opt.extractFromAliases(table), ["A", "B"]);
});

  it("remaps case statements");
  it("remaps ops");

  it('optimizes simple non-aggr select', function() {
    /*
    select a.a1 as s1, (select b.b1 from b as b where b.b2 = a.a2) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt1.opt_a_a1 as s1, opt1.expr as s2
    from
    ****** First query opt1 that does left outer join
    (
      select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, opt0.b1 as expr from a as a
      left outer join b as opt0 on opt0.b2 = a.a2
    ) as opt1
    order by opt1.opt_a_a3

    */
    const input = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" },
        { 
          type: "select",
          expr: { 
            type: "scalar",
            expr: { type: "field", tableAlias: "b", column: "b1" },
            from: { type: "table", table: "b", alias: "b" },
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          },
          alias: "s2" 
        }
      ],
      from: { type: "table", table: "a", alias: "a" },
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    };

    const output = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "s1" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "expr" }, alias: "s2" }
      ],
      from: {
        type: "subquery",
        query: {
          type: "query",
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" },
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" },
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" },
            { type: "select", expr: { type: "field", tableAlias: "opt0", column: "b1" }, alias: "expr" }
          ],
          from: { 
            type: "join",
            kind: "left",
            left: { type: "table", table: "a", alias: "a" },
            right: { type: "table", table: "b", alias: "opt0" },
            on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "opt0", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          },
          where: null
        },
        alias: "opt1"
      },
      where: null,
      orderBy: [{ expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }}]
    };

    return compare(this.opt.rewriteScalar(input), output);
  });

  it('optimizes simple aggr select', function() {
    /*
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt2.opt_a_a1 as s1, opt2.expr as s2
    from
    ****** Second query opt2 that does left outer join 
    (
      select opt1.rn as rn, opt1.opt_a_a1 as opt_a_a1, opt1.opt_a_a2 as opt_a_a2, opt1.opt_a_a3 as opt_a_a3, sum(opt1.b1) as expr
      from 
      ****** First query opt1 that adds row number 
      (
        select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, row_number() over () as rn from a as a
      ) as opt1
      left outer join b as opt0 on opt0.b2 = opt1.opt_a_a2
      ****** group by all fields except expr 
      group by 1, 2, 3, 4
    ) as opt2
    order by opt2.opt_a_a3

    */
    const input = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" },
        { 
          type: "select",
          expr: { 
            type: "scalar",
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] },
            from: { type: "table", table: "b", alias: "b" },
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          },
          alias: "s2" 
        }
      ],
      from: { type: "table", table: "a", alias: "a" },
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    };

    const inner0Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ],
      from: { type: "table", table: "a", alias: "a" },
      where: null
    };

    const output = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a1" }, alias: "s1" },
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "expr" }, alias: "s2" }
      ],
      from: {
        type: "subquery",
        query: {
          type: "query",
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "rn" }, alias: "rn" },
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "opt_a_a1" },
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a2" }, alias: "opt_a_a2" },
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }, alias: "opt_a_a3" },
            { type: "select", expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "opt0", column: "b1" }] }, alias: "expr" }
          ],
          from: { 
            type: "join",
            kind: "left",
            left: { type: "subquery", query: inner0Query, alias: "opt1" },
            right: { type: "table", table: "b", alias: "opt0" },
            on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "opt0", column: "b2" }, { type: "field", tableAlias: "opt1", column: "opt_a_a2" }] }
          },
          groupBy: [1, 2, 3, 4]
        },
        alias: "opt2"
      },
      where: null,
      orderBy: [{ expr: { type: "field", tableAlias: "opt2", column: "opt_a_a3" }}]
    };

    // console.log JSON.stringify(output, null, 2)
    return compare(this.opt.rewriteScalar(input), output);
  });

  it('optimizes simple limit select', function() {
    /*
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2 order by b.b3 limit 1) as s2
    from a as a
    order by a.a3

    should be optimized to:
    
    select opt3.opt_a_a1 as s1, opt3.expr as s2
    from
    ****** Third query opt3 that removes all but top row of window 
    (
      select opt2.opt_a_a1 as opt_a_a1, opt2.opt_a_a2 as opt_a_a2, opt2.opt_a_a3 as opt_a_a3, expr as expr
      from 
      ****** Second query opt2 that does left outer join and adds row number 
      (
        select opt1.opt_a_a1 as opt_a_a1, opt1.opt_a_a2 as opt_a_a2, opt1.opt_a_a3 as opt_a_a3, opt0.b1 as expr, row_number() over (partition by opt1.rn order by opt0.b3) as rn
        from 
        ****** First query opt1 that adds row number 
        (
          select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, row_number() over () as rn from a as a
        ) as opt1
        left outer join b as opt0 on opt0.b2 = opt1.opt_a_a2
      ) as opt2
      where opt2.rn = 1
    ) as opt3
    order by opt3.opt_a_a3

    */
    const input = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" },
        { 
          type: "select",
          expr: { 
            type: "scalar",
            expr: { type: "field", tableAlias: "b", column: "b1" },
            from: { type: "table", table: "b", alias: "b" },
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] },
            orderBy: [{ expr: { type: "field", tableAlias: "b", column: "b3" }, direction: "asc" }],
            limit: 1
          },
          alias: "s2" 
        }
      ],
      from: { type: "table", table: "a", alias: "a" },
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    };

    const inner0Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ],
      from: { type: "table", table: "a", alias: "a" },
      where: null
    };
    
    const inner1Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "b1" }, alias: "expr" },
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: { 
          partitionBy: [{ type: "field", tableAlias: "opt1", column: "rn" }],
          orderBy: [{ expr: { type: "field", tableAlias: "opt0", column: "b3" }, direction: "asc" }]
        }, alias: "rn" }
      ],
      from: { 
        type: "join",
        kind: "left",
        left: { type: "subquery", query: inner0Query, alias: "opt1" },
        right: { type: "table", table: "b", alias: "opt0" },
        on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "opt0", column: "b2" }, { type: "field", tableAlias: "opt1", column: "opt_a_a2" }] }
      }
    };

    const inner2Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "expr" }, alias: "expr" }
      ],
      from: { type: "subquery", query: inner1Query, alias: "opt2" },
      where: {
        type: "op",
        op: "=",
        exprs: [
          { type: "field", tableAlias: "opt2", column: "rn" },
          { type: "literal", value: 1 }
        ]
      }
    };

    const output = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt3", column: "opt_a_a1" }, alias: "s1" },
        { type: "select", expr: { type: "field", tableAlias: "opt3", column: "expr" }, alias: "s2" }
      ],
      from: {
        type: "subquery",
        query: inner2Query,
        alias: "opt3"
      },
      where: null,
      orderBy: [{ expr: { type: "field", tableAlias: "opt3", column: "opt_a_a3" }}]
    };

    return compare(this.opt.rewriteScalar(input), output);
  });

  it('optimizes aggr select with where', function() {
    /*
    select a.a1 as s1, (select sum(b.b1) from b as b where b.b2 = a.a2) as s2
    from a as a
    where (select sum(c.c1) from c as c where c.c2 = a.a4) = 2 and a.a5 = 3
    order by a.a3

    should be optimized to (after one pass): 
    
    select opt2.opt_a_a1 as s1, (select sum(b.b1) from b as b where b.b2 = opt2.a_a2) as s2
    from
    ****** Second query opt2 that does left outer join 
    (
      select opt1.rn as rn, opt1.opt_a_a1 as opt_a_a1, opt1.opt_a_a2 as opt_a_a2, opt1.opt_a_a3 as opt_a_a3, sum(c.c1) as expr
      from 
      ****** First query opt1 that adds row number and does eligible wheres
      (
        select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, a.a4 as opt_a_a4, row_number() over () as rn 
        from a as a
        where a.a5 = 3
      ) as opt1
      left outer join c as c on c.c2 = opt1.opt_a_a4
      ****** group by all fields except expr 
      group by 1, 2, 3, 4
    ) as opt2
    where opt2.expr = 2
    order by opt2.opt_a_a3

    */
    const input = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" },
        { 
          type: "select",
          expr: { 
            type: "scalar",
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] },
            from: { type: "table", table: "b", alias: "b" },
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
          },
          alias: "s2" 
        }
      ],
      from: { type: "table", table: "a", alias: "a" },
      where: {
        type: "op",
        op: "and",
        exprs: [
          { type: "op", op: "=", exprs: [
            { 
              type: "scalar",
              expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "c", column: "c1" }] },
              from: { type: "table", table: "c", alias: "c" },
              where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "c", column: "c2" }, { type: "field", tableAlias: "a", column: "a4" }] }
            },
            { type: "literal", value: 2 }
          ]},
          { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "a", column: "a5" }, { type: "literal", value: 3 }] }
        ]
      },
      orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
    };

    const inner0Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a4" }, alias: "opt_a_a4" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a5" }, alias: "opt_a_a5" },
        { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" }
      ],
      from: { type: "table", table: "a", alias: "a" },
      where: {
        type: "op",
        op: "and",
        exprs: [
          { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "a", column: "a5" }, { type: "literal", value: 3 }] }
        ]
      }
    };

    const inner1Query = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "rn" }, alias: "rn" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "opt_a_a1" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a2" }, alias: "opt_a_a2" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a4" }, alias: "opt_a_a4" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a5" }, alias: "opt_a_a5" },
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }, alias: "opt_a_a3" },
        { type: "select", expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "opt0", column: "c1" }] }, alias: "expr" }
      ],
      from: { 
        type: "join",
        kind: "left",
        left: { type: "subquery", query: inner0Query, alias: "opt1" },
        right: { type: "table", table: "c", alias: "opt0" },
        on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "opt0", column: "c2" }, { type: "field", tableAlias: "opt1", column: "opt_a_a4" }] }
      },
      groupBy: [1, 2, 3, 4, 5, 6]
    };

    const output = {
      type: "query",
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "opt2", column: "opt_a_a1" }, alias: "s1" },
        { 
          type: "select",
          expr: { 
            type: "scalar",
            expr: { type: "op", op: "sum", exprs: [{ type: "field", tableAlias: "b", column: "b1" }] },
            from: { type: "table", table: "b", alias: "b" },
            where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "opt2", column: "opt_a_a2" }] }
          },
          alias: "s2" 
        }
      ],
      from: {
        type: "subquery",
        query: inner1Query,
        alias: "opt2"
      },
      where: {
        type: "op",
        op: "and",
        exprs: [
          { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "opt2", column: "expr" },
            { type: "literal", value: 2 }        
          ]}
        ]
      },
      orderBy: [{ expr: { type: "field", tableAlias: "opt2", column: "opt_a_a3" }}]
    };

    // console.log JSON.stringify(output, null, 2)
    return compare(this.opt.rewriteScalar(input), output);
  });

  return it('optimizes inner query simple non-aggr select', function() {
    /*
    select x.s1 as x1 from (
      select a.a1 as s1, (select b.b1 from b as b where b.b2 = a.a2) as s2
      from a as a
      order by a.a3
    ) as x

    should be optimized to:
    
    select x.s1 as x1 from (
      select opt1.opt_a_a1 as s1, opt1.expr as s2
      from
      ****** First query opt1 that does left outer join
      (
        select a.a1 as opt_a_a1, a.a2 as opt_a_a2, a.a3 as opt_a_a3, opt0.b1 as expr from a as a
        left outer join b as opt0 on opt0.b2 = a.a2
      ) as opt1
      order by opt1.opt_a_a3
    ) as x

    */
    const input = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "x", column: "s1"}, alias: "x1" }],
      from: {
        type: "subquery",
        query: {
          type: "query",
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "s1" },
            { 
              type: "select",
              expr: { 
                type: "scalar",
                expr: { type: "field", tableAlias: "b", column: "b1" },
                from: { type: "table", table: "b", alias: "b" },
                where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "b", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
              },
              alias: "s2" 
            }
          ],
          from: { type: "table", table: "a", alias: "a" },
          orderBy: [{ expr: { type: "field", tableAlias: "a", column: "a3" }}]
        },
        alias: "x"
      }
    };

    const output = {
      type: "query",
      selects: [{ type: "select", expr: { type: "field", tableAlias: "x", column: "s1"}, alias: "x1" }],
      from: {
        type: "subquery",
        query: {
          type: "query",
          selects: [
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_a_a1" }, alias: "s1" },
            { type: "select", expr: { type: "field", tableAlias: "opt1", column: "expr" }, alias: "s2" }
          ],
          from: {
            type: "subquery",
            query: {
              type: "query",
              selects: [
                { type: "select", expr: { type: "field", tableAlias: "a", column: "a1" }, alias: "opt_a_a1" },
                { type: "select", expr: { type: "field", tableAlias: "a", column: "a2" }, alias: "opt_a_a2" },
                { type: "select", expr: { type: "field", tableAlias: "a", column: "a3" }, alias: "opt_a_a3" },
                { type: "select", expr: { type: "field", tableAlias: "opt0", column: "b1" }, alias: "expr" }
              ],
              from: { 
                type: "join",
                kind: "left",
                left: { type: "table", table: "a", alias: "a" },
                right: { type: "table", table: "b", alias: "opt0" },
                on: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "opt0", column: "b2" }, { type: "field", tableAlias: "a", column: "a2" }] }
              },
              where: null
            },
            alias: "opt1"
          },
          where: null,
          orderBy: [{ expr: { type: "field", tableAlias: "opt1", column: "opt_a_a3" }}]
        },
        alias: "x"
      }
    };

    return compare(this.opt.rewriteScalar(input), output);
  });
});
