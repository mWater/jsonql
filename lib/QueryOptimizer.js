"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
/*

Scalar subqueries can be very slow in Postgresql as they are not re-written but instead loop over and over.

This attempts to re-write them as left outer joins, which is a complex tranformation.

There are three cases:

1) non-aggregated subquery. These are just a left outer join in an inner query

2) aggregated subquery. These are sum(some value), etc as the thing being selected in the scalar

3) limit 1 subquery. These are taking the *latest* of some value, for example, and have an order by and limit 1

When the scalar is in the where clause, the where clause is processed first, splitting into "ands" and putting
as much as possible in the inner query for speed.

We need to do trickery with row_number to give the wrapping queries something to group on or partition by.

We re-write wheres first, followed by selects, followed by order bys.

See the tests for examples of all three re-writings. The speed difference is 1000x plus depending on the # of rows.

*/
class QueryOptimizer {
    constructor() {
        // Extract all jsonql field expressions from a jsonql fragment
        this.extractFields = (frag) => {
            if (!frag || !frag.type) {
                return [];
            }
            switch (frag.type) {
                case "query":
                    return lodash_1.default.flatten(lodash_1.default.map(frag.selects, (select) => this.extractFields(select.expr)))
                        .concat(this.extractFields(frag.where))
                        .concat(lodash_1.default.flatten(lodash_1.default.map(frag.orderBy, (orderBy) => this.extractFields(orderBy.expr))));
                case "field":
                    return [frag];
                case "op":
                    return lodash_1.default.flatten(lodash_1.default.map(frag.exprs, this.extractFields));
                case "case":
                    return this.extractFields(frag.input)
                        .concat(lodash_1.default.flatten(lodash_1.default.map(frag.cases, (cs) => this.extractFields(cs.when).concat(this.extractFields(cs.then)))))
                        .concat(this.extractFields(frag.else));
                case "scalar":
                    return this.extractFields(frag.frag)
                        .concat(this.extractFields(frag.where))
                        .concat(lodash_1.default.map(frag.orderBy, (ob) => this.extractFields(ob.frag)));
                case "literal":
                    return [];
                case "token":
                    return [];
                default:
                    throw new Error(`Unsupported extractFields with type ${frag.type}`);
            }
        };
        // Determine if expression is aggregate
        this.isAggr = (expr) => {
            if (!expr || !expr.type) {
                return false;
            }
            switch (expr.type) {
                case "field":
                    return false;
                case "op":
                    if (["sum", "min", "max", "avg", "count", "stdev", "stdevp", "var", "varp", "array_agg"].includes(expr.op)) {
                        return true;
                    }
                    return lodash_1.default.any(expr.exprs, (ex) => this.isAggr(ex));
                case "case":
                    return lodash_1.default.any(expr.cases, (cs) => this.isAggr(cs.then));
                case "scalar":
                    return false;
                case "literal":
                    return false;
                case "token":
                    return false;
                default:
                    throw new Error(`Unsupported isAggr with type ${expr.type}`);
            }
        };
        this.aliasNum = 0;
    }
    debugQuery(query) {
        const SchemaMap = require("./SchemaMap");
        const JsonqlCompiler = require("./JsonqlCompiler");
        try {
            const sql = new JsonqlCompiler(new SchemaMap(), false).compileQuery(query);
            console.log("===== SQL ======");
            console.log(sql.toInline());
            return console.log("================");
        }
        catch (ex) {
            console.log("FAILURE: " + ex.message);
            return console.log(JSON.stringify(query, null, 2));
        }
    }
    // Run rewriteScalar query repeatedly until no more changes
    optimizeQuery(query, debug = false) {
        if (debug) {
            console.log("================== BEFORE OPT ================");
            this.debugQuery(query);
        }
        for (let i = 0; i < 20; i++) {
            const optQuery = this.rewriteScalar(query);
            if (lodash_1.default.isEqual(optQuery, query)) {
                return optQuery;
            }
            if (debug) {
                console.log(`================== OPT ${i} ================`);
                this.debugQuery(optQuery);
            }
            query = optQuery;
        }
        throw new Error(`Unable to optimize query (infinite loop): ${JSON.stringify(query)}`);
    }
    rewriteScalar(query) {
        // First optimize any inner queries
        let opt1Alias, opt1Query, opt1Selects, opt2Alias, opt2From, opt2Query, opt2Selects, outerQuery;
        query = this.optimizeInnerQueries(query);
        // Find scalar to optimize
        const scalar = this.findScalar(query);
        // If no scalar to optimize, return
        if (!scalar) {
            return query;
        }
        // If scalar doesn't have simply aliases from, return
        if (!scalar.from.alias) {
            return query;
        }
        const oldScalarAlias = scalar.from.alias;
        const newScalarAlias = this.createAlias();
        // Get table aliases in from
        const fromAliases = this.extractFromAliases(query.from);
        // Get all fields
        let fields = this.extractFields(query);
        // Filter fields to ones that reference from clause
        fields = lodash_1.default.filter(fields, (f) => fromAliases.includes(f.tableAlias));
        // Unique fields
        fields = lodash_1.default.uniq(fields, (f) => `${f.tableAlias}::${f.column}`);
        // Split where into ands
        let wheres = [];
        if (query.where && query.where.type === "op" && query.where.op === "and") {
            wheres = query.where.exprs;
        }
        else if (query.where) {
            // Single expression
            wheres = [query.where];
        }
        // Split inner where (not containing the scalar) and outer wheres (containing the scalar)
        let innerWhere = {
            type: "op",
            op: "and",
            exprs: lodash_1.default.filter(wheres, (where) => {
                return this.findScalar(where) !== scalar;
            })
        };
        let outerWhere = {
            type: "op",
            op: "and",
            exprs: lodash_1.default.filter(wheres, (where) => {
                return this.findScalar(where) === scalar;
            })
        };
        // Null if empty
        if (innerWhere.exprs.length === 0) {
            innerWhere = null;
        }
        if (outerWhere.exprs.length === 0) {
            outerWhere = null;
        }
        // Remaps over clause in select
        const remapOver = (over, alias) => {
            if (!over) {
                return over;
            }
            return lodash_1.default.omit({
                partitionBy: over.partitionBy
                    ? lodash_1.default.map(over.partitionBy, (pb) => this.remapFields(pb, fields, scalar, alias))
                    : undefined,
                orderBy: over.orderBy
                    ? lodash_1.default.map(over.orderBy, (ob) => lodash_1.default.extend({}, ob, { expr: this.remapFields(ob.expr, fields, scalar, alias) }))
                    : undefined
            }, lodash_1.default.isUndefined);
        };
        // Remaps selects for outer query, mapping fields in expr and over clauses
        const remapSelects = (selects, alias) => {
            // Re-write query selects to use new opt1 query
            return lodash_1.default.map(selects, (select) => {
                // Get rid of undefined values
                return lodash_1.default.omit({
                    type: "select",
                    expr: this.remapFields(select.expr, fields, scalar, alias),
                    over: remapOver(select.over, alias),
                    alias: select.alias
                }, lodash_1.default.isUndefined);
            });
        };
        // If simple non-aggregate
        if (!this.isAggr(scalar.expr) && !scalar.limit) {
            // Create new selects for opt1 query with all fields + scalar expression
            opt1Selects = lodash_1.default.map(fields, (field) => {
                return { type: "select", expr: field, alias: `opt_${field.tableAlias}_${field.column}` };
            });
            opt1Selects.push({
                type: "select",
                expr: this.changeAlias(scalar.expr, oldScalarAlias, newScalarAlias),
                alias: "expr"
            });
            // Create new opt1 from clause with left outer join to scalar
            const opt1From = {
                type: "join",
                kind: "left",
                left: query.from,
                right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
                on: this.changeAlias(scalar.where, oldScalarAlias, newScalarAlias)
            };
            // Create opt1 query opt1
            opt1Query = {
                type: "query",
                selects: opt1Selects,
                from: opt1From,
                where: innerWhere
            };
            // Optimize inner query
            opt1Query = this.optimizeQuery(opt1Query, false);
            // Create alias for opt1 query
            opt1Alias = this.createAlias();
            outerQuery = lodash_1.default.extend({}, query, {
                // Re-write query selects to use new opt1 query
                selects: remapSelects(query.selects, opt1Alias),
                from: {
                    type: "subquery",
                    query: opt1Query,
                    alias: opt1Alias
                },
                where: this.remapFields(outerWhere, fields, scalar, opt1Alias),
                orderBy: lodash_1.default.map(query.orderBy, (orderBy) => {
                    if (!orderBy.expr) {
                        return orderBy;
                    }
                    return lodash_1.default.extend({}, orderBy, { expr: this.remapFields(orderBy.expr, fields, scalar, opt1Alias) });
                })
            });
            return outerQuery;
        }
        else if (!scalar.limit) {
            // Create new selects for opt1 query with all fields + row number
            opt1Selects = lodash_1.default.map(fields, (field) => {
                return { type: "select", expr: field, alias: `opt_${field.tableAlias}_${field.column}` };
            });
            opt1Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" });
            // Create alias for opt1 query
            opt1Alias = this.createAlias();
            // Create opt1 query opt1
            opt1Query = {
                type: "query",
                selects: opt1Selects,
                from: query.from,
                where: innerWhere
            };
            // Optimize inner query
            opt1Query = this.optimizeQuery(opt1Query, false);
            // Create new selects for opt2 query with row number + all fields + scalar expression
            opt2Selects = [{ type: "select", expr: { type: "field", tableAlias: opt1Alias, column: "rn" }, alias: "rn" }];
            opt2Selects = opt2Selects.concat(lodash_1.default.map(fields, (field) => {
                return {
                    type: "select",
                    expr: { type: "field", tableAlias: opt1Alias, column: `opt_${field.tableAlias}_${field.column}` },
                    alias: `opt_${field.tableAlias}_${field.column}`
                };
            }));
            opt2Selects.push({
                type: "select",
                expr: this.changeAlias(this.remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias),
                alias: "expr"
            });
            // Create new opt2 from clause with left outer join to scalar
            opt2From = {
                type: "join",
                kind: "left",
                left: { type: "subquery", query: opt1Query, alias: opt1Alias },
                right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
                on: this.changeAlias(this.remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
            };
            opt2Query = {
                type: "query",
                selects: opt2Selects,
                from: opt2From,
                groupBy: lodash_1.default.range(1, fields.length + 2)
            };
            // Create alias for opt2 query
            opt2Alias = this.createAlias();
            outerQuery = lodash_1.default.extend({}, query, {
                // Re-write query selects to use new opt2 query
                selects: remapSelects(query.selects, opt2Alias),
                from: {
                    type: "subquery",
                    query: opt2Query,
                    alias: opt2Alias
                },
                where: this.remapFields(outerWhere, fields, scalar, opt2Alias),
                orderBy: lodash_1.default.map(query.orderBy, (orderBy) => {
                    if (!orderBy.expr) {
                        return orderBy;
                    }
                    return lodash_1.default.extend({}, orderBy, { expr: this.remapFields(orderBy.expr, fields, scalar, opt2Alias) });
                })
            });
            return outerQuery;
            // Limit scalar
        }
        else {
            // Create new selects for opt1 query with all fields + row number
            opt1Selects = lodash_1.default.map(fields, (field) => {
                return { type: "select", expr: field, alias: `opt_${field.tableAlias}_${field.column}` };
            });
            opt1Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" });
            // Create opt1 query opt1
            opt1Query = {
                type: "query",
                selects: opt1Selects,
                from: query.from,
                where: innerWhere
            };
            // Optimize inner query
            opt1Query = this.optimizeQuery(opt1Query, false);
            // Create alias for opt1 query
            opt1Alias = this.createAlias();
            // Create new selects for opt2 query with all fields + scalar expression + ordered row number over inner row number
            opt2Selects = lodash_1.default.map(fields, (field) => {
                return {
                    type: "select",
                    expr: { type: "field", tableAlias: opt1Alias, column: `opt_${field.tableAlias}_${field.column}` },
                    alias: `opt_${field.tableAlias}_${field.column}`
                };
            });
            opt2Selects.push({
                type: "select",
                expr: this.changeAlias(this.remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias),
                alias: "expr"
            });
            opt2Selects.push({
                type: "select",
                expr: { type: "op", op: "row_number", exprs: [] },
                over: {
                    partitionBy: [{ type: "field", tableAlias: opt1Alias, column: "rn" }],
                    orderBy: lodash_1.default.map(scalar.orderBy, (ob) => {
                        if (ob.expr) {
                            return lodash_1.default.extend({}, ob, { expr: this.changeAlias(ob.expr, oldScalarAlias, newScalarAlias) });
                        }
                        return ob;
                    })
                },
                alias: "rn"
            });
            // Create new opt2 from clause with left outer join to scalar
            opt2From = {
                type: "join",
                kind: "left",
                left: { type: "subquery", query: opt1Query, alias: opt1Alias },
                right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
                on: this.changeAlias(this.remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
            };
            opt2Query = {
                type: "query",
                selects: opt2Selects,
                from: opt2From
            };
            // Create alias for opt2 query
            opt2Alias = this.createAlias();
            const opt3Selects = lodash_1.default.map(fields, (field) => {
                return {
                    type: "select",
                    expr: { type: "field", tableAlias: opt2Alias, column: `opt_${field.tableAlias}_${field.column}` },
                    alias: `opt_${field.tableAlias}_${field.column}`
                };
            });
            opt3Selects.push({
                type: "select",
                expr: { type: "field", tableAlias: opt2Alias, column: "expr" },
                alias: "expr"
            });
            const opt3Query = {
                type: "query",
                selects: opt3Selects,
                from: {
                    type: "subquery",
                    query: opt2Query,
                    alias: opt2Alias
                },
                where: {
                    type: "op",
                    op: "=",
                    exprs: [
                        { type: "field", tableAlias: opt2Alias, column: "rn" },
                        { type: "literal", value: 1 }
                    ]
                }
            };
            // Create alias for opt3 query
            const opt3Alias = this.createAlias();
            // Wrap in final query
            outerQuery = lodash_1.default.extend({}, query, {
                // Re-write query selects to use new opt2 query
                selects: remapSelects(query.selects, opt3Alias),
                from: {
                    type: "subquery",
                    query: opt3Query,
                    alias: opt3Alias
                },
                where: this.remapFields(outerWhere, fields, scalar, opt3Alias),
                orderBy: lodash_1.default.map(query.orderBy, (orderBy) => {
                    if (!orderBy.expr) {
                        return orderBy;
                    }
                    return lodash_1.default.extend({}, orderBy, { expr: this.remapFields(orderBy.expr, fields, scalar, opt3Alias) });
                })
            });
            return outerQuery;
        }
    }
    optimizeInnerQueries(query) {
        var optimizeFrom = (from) => {
            switch (from.type) {
                case "table":
                case "subexpr":
                    return from;
                case "join":
                    return lodash_1.default.extend({}, from, {
                        left: optimizeFrom(from.left),
                        right: optimizeFrom(from.right)
                    });
                case "subquery":
                    return lodash_1.default.extend({}, from, {
                        query: this.optimizeQuery(from.query)
                    });
                default:
                    throw new Error(`Unknown optimizeFrom type ${from.type}`);
            }
        };
        return (query = lodash_1.default.extend({}, query, { from: optimizeFrom(query.from) }));
    }
    // Find a scalar in where, selects or order by or expression
    findScalar(frag) {
        if (!frag || !frag.type) {
            return null;
        }
        switch (frag.type) {
            case "query":
                // Find in where clause
                var scalar = this.findScalar(frag.where);
                if (scalar) {
                    return scalar;
                }
                // Find in selects
                for (let select of frag.selects) {
                    scalar = this.findScalar(select.expr);
                    if (scalar) {
                        return scalar;
                    }
                }
                // Find in order by
                if (frag.orderBy) {
                    for (let orderBy of frag.orderBy) {
                        scalar = this.findScalar(orderBy.expr);
                        if (scalar) {
                            return scalar;
                        }
                    }
                }
                break;
            case "scalar":
                return frag;
                break;
            case "op":
                if (frag.exprs) {
                    for (let expr of frag.exprs) {
                        scalar = this.findScalar(expr);
                        if (scalar) {
                            return scalar;
                        }
                    }
                }
                break;
        }
        return null;
    }
    // Change a specific alias to another one
    changeAlias(frag, fromAlias, toAlias) {
        if (!frag || !frag.type) {
            return frag;
        }
        switch (frag.type) {
            case "field":
                if (frag.tableAlias === fromAlias) {
                    // Remap
                    return { type: "field", tableAlias: toAlias, column: frag.column };
                }
                return frag;
            case "op":
                var newFrag = lodash_1.default.extend({}, frag, {
                    exprs: lodash_1.default.map(frag.exprs, (ex) => this.changeAlias(ex, fromAlias, toAlias))
                });
                if (frag.orderBy) {
                    newFrag.orderBy = lodash_1.default.map(frag.orderBy, (ob) => {
                        if (ob.expr) {
                            return lodash_1.default.extend({}, ob, { expr: this.changeAlias(ob.expr, fromAlias, toAlias) });
                        }
                        return ob;
                    });
                }
                return newFrag;
            case "case":
                return lodash_1.default.extend({}, frag, {
                    input: this.changeAlias(frag.input, fromAlias, toAlias),
                    cases: lodash_1.default.map(frag.cases, (cs) => {
                        return {
                            when: this.changeAlias(cs.when, fromAlias, toAlias),
                            then: this.changeAlias(cs.then, fromAlias, toAlias)
                        };
                    }),
                    else: this.changeAlias(frag.else, fromAlias, toAlias)
                });
            case "scalar":
                newFrag = lodash_1.default.extend({}, frag, {
                    expr: this.changeAlias(frag.expr, fromAlias, toAlias),
                    from: this.changeAlias(frag.from, fromAlias, toAlias),
                    where: this.changeAlias(frag.where, fromAlias, toAlias),
                    orderBy: this.changeAlias(frag.orderBy, fromAlias, toAlias)
                });
                if (frag.orderBy) {
                    newFrag.orderBy = lodash_1.default.map(frag.orderBy, (ob) => {
                        if (ob.expr) {
                            return lodash_1.default.extend({}, ob, { expr: this.changeAlias(ob.expr, fromAlias, toAlias) });
                        }
                        return ob;
                    });
                }
                return newFrag;
            case "table":
                if (frag.alias === fromAlias) {
                    return { type: "table", table: frag.table, alias: toAlias };
                }
                return frag;
            case "join":
                return lodash_1.default.extend({}, frag, {
                    left: this.changeAlias(frag.left, fromAlias, toAlias),
                    right: this.changeAlias(frag.right, fromAlias, toAlias),
                    on: this.changeAlias(frag.on, fromAlias, toAlias)
                });
            case "literal":
                return frag;
            case "token":
                return frag;
            default:
                throw new Error(`Unsupported changeAlias with type ${frag.type}`);
        }
    }
    extractFromAliases(from) {
        switch (from.type) {
            case "table":
            case "subquery":
            case "subexpr":
                return [from.alias];
                break;
            case "join":
                return this.extractFromAliases(from.left).concat(this.extractFromAliases(from.right));
                break;
        }
        throw new Error(`Unknown from type ${from.type}`);
    }
    // Remap fields a.b1 to format <tableAlias>.opt_a_b1
    remapFields(frag, fields, scalar, tableAlias) {
        if (!frag || !frag.type) {
            return frag;
        }
        switch (frag.type) {
            case "field":
                for (let field of fields) {
                    // Remap
                    if (field.tableAlias === frag.tableAlias && field.column === frag.column) {
                        return { type: "field", tableAlias, column: `opt_${field.tableAlias}_${field.column}` };
                    }
                }
                return frag;
            case "op":
                return lodash_1.default.extend({}, frag, {
                    exprs: lodash_1.default.map(frag.exprs, (ex) => this.remapFields(ex, fields, scalar, tableAlias))
                });
            case "case":
                return lodash_1.default.extend({}, frag, {
                    input: this.remapFields(frag.input, fields, scalar, tableAlias),
                    cases: lodash_1.default.map(frag.cases, (cs) => {
                        return {
                            when: this.remapFields(cs.when, fields, scalar, tableAlias),
                            then: this.remapFields(cs.then, fields, scalar, tableAlias)
                        };
                    }),
                    else: this.remapFields(frag.else, fields, scalar, tableAlias)
                });
            case "scalar":
                if (scalar === frag) {
                    return { type: "field", tableAlias, column: "expr" };
                }
                else {
                    const newFrag = lodash_1.default.extend({}, frag, {
                        expr: this.remapFields(frag.expr, fields, scalar, tableAlias),
                        from: this.remapFields(frag.from, fields, scalar, tableAlias),
                        where: this.remapFields(frag.where, fields, scalar, tableAlias)
                    });
                    if (frag.orderBy) {
                        newFrag.orderBy = lodash_1.default.map(frag.orderBy, (ob) => {
                            if (ob.expr) {
                                return lodash_1.default.extend({}, ob, { expr: this.remapFields(ob.expr, fields, scalar, tableAlias) });
                            }
                            return ob;
                        });
                    }
                    return newFrag;
                }
            case "table":
                return frag;
            case "join":
                return lodash_1.default.extend({}, frag, {
                    left: this.remapFields(frag.left, fields, scalar, tableAlias),
                    right: this.remapFields(frag.right, fields, scalar, tableAlias),
                    on: this.remapFields(frag.on, fields, scalar, tableAlias)
                });
            case "literal":
                return frag;
            case "token":
                return frag;
            default:
                throw new Error(`Unsupported remapFields with type ${frag.type}`);
        }
    }
    // Create a unique table alias
    createAlias() {
        const alias = `opt${this.aliasNum}`;
        this.aliasNum += 1;
        return alias;
    }
}
exports.default = QueryOptimizer;
// replaceFrag: (frag, fromFrag, toFrag) ->
//   if not frag or not frag.type
//     return frag
//   if frag == from
//     return to
//   switch frag.type
//     when "query"
//       return _.extend({}, frag,
//         selects: _.map(frag.selects, (ex) => @replaceFrag(ex, fromFrag, toFrag)))
//         from: @replaceFrag(frag.from, fromFrag, toFrag)
//         where: @replaceFrag(frag.where, fromFrag, toFrag)
//         orderBy: @replaceFrag(frag.where, fromFrag, toFrag)
//         )
//     when "field"
//       return frag
//     when "op"
//       return _.extend({}, frag, exprs: _.map(frag.exprs, (ex) => @replaceFrag(ex, fromFrag, toFrag)))
//     when "case"
//       return _.extend({}, frag, {
//         input: @replaceFrag(frag.input, fromFrag, toFrag)
//         cases: _.map(frag.cases, (cs) =>
//           {
//             when: @replaceFrag(cs.when, fromFrag, toFrag)
//             then: @replaceFrag(cs.then, fromFrag, toFrag)
//           }
//         )
//         else: @replaceFrag(frag.else, fromFrag, toFrag)
//       })
//     when "scalar"
//       return _.extend({}, frag, {
//         expr: @replaceFrag(frag.expr, fromFrag, toFrag)
//         from: @replaceFrag(frag.from, fromFrag, toFrag)
//         where: @replaceFrag(frag.where, fromFrag, toFrag)
//         orderBy: @replaceFrag(frag.orderBy, fromFrag, toFrag)
//       })
//     when "table"
//       if frag.alias == fromFrag
//         return { type: "table", table: frag.table, alias: toFrag }
//       return frag
//     when "join"
//       return _.extend({}, frag, {
//         left: @replaceFrag(frag.left, fromFrag, toFrag)
//         right: @replaceFrag(frag.right, fromFrag, toFrag)
//         on: @replaceFrag(frag.on, fromFrag, toFrag)
//       })
//     when "literal"
//       return frag
//     when "token"
//       return frag
//     else
//       throw new Error("Unsupported replaceFrag with type #{frag.type}")
