var QueryOptimizer, _,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');


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

module.exports = QueryOptimizer = (function() {
  function QueryOptimizer() {
    this.isAggr = bind(this.isAggr, this);
    this.extractFields = bind(this.extractFields, this);
  }

  QueryOptimizer.prototype.optimizeQuery = function(query) {
    var i, j, optQuery;
    for (i = j = 0; j < 20; i = ++j) {
      optQuery = this.rewriteScalar(query);
      if (optQuery === query) {
        return optQuery;
      }
      query = optQuery;
    }
    throw new Error("Unable to optimize query (infinite loop): " + (JSON.stringify(query)));
  };

  QueryOptimizer.prototype.rewriteScalar = function(query) {
    var fields, fromAliases, innerWhere, opt0From, opt0Query, opt0Selects, opt1From, opt1Query, opt1Selects, opt2Query, opt2Selects, outerQuery, outerWhere, remapOver, remapSelects, scalar, wheres;
    scalar = this.findScalar(query);
    if (!scalar) {
      return query;
    }
    fromAliases = this.extractFromAliases(query.from);
    fields = this.extractFields(query);
    fields = _.filter(fields, function(f) {
      var ref;
      return ref = f.tableAlias, indexOf.call(fromAliases, ref) >= 0;
    });
    fields = _.uniq(fields, function(f) {
      return f.tableAlias + "::" + f.column;
    });
    wheres = [];
    if (query.where && query.where.type === "op" && query.where.op === "and") {
      wheres = query.where.exprs;
    } else if (query.where) {
      wheres = [query.where];
    }
    innerWhere = {
      type: "op",
      op: "and",
      exprs: _.filter(wheres, (function(_this) {
        return function(where) {
          return _this.findScalar(where) !== scalar;
        };
      })(this))
    };
    outerWhere = {
      type: "op",
      op: "and",
      exprs: _.filter(wheres, (function(_this) {
        return function(where) {
          return _this.findScalar(where) === scalar;
        };
      })(this))
    };
    if (innerWhere.exprs.length === 0) {
      innerWhere = null;
    }
    if (outerWhere.exprs.length === 0) {
      outerWhere = null;
    }
    remapOver = (function(_this) {
      return function(over, alias) {
        if (!over) {
          return over;
        }
        return _.omit({
          partitionBy: over.partitionBy ? _.map(over.partitionBy, function(pb) {
            return _this.remapFields(pb, fields, scalar, alias);
          }) : void 0,
          orderBy: over.orderBy ? _.map(over.orderBy, function(ob) {
            return _.extend({}, ob, {
              expr: _this.remapFields(ob.expr, fields, scalar, alias)
            });
          }) : void 0
        }, _.isUndefined);
      };
    })(this);
    remapSelects = (function(_this) {
      return function(selects, alias) {
        return _.map(selects, function(select) {
          return _.omit({
            type: "select",
            expr: _this.remapFields(select.expr, fields, scalar, alias),
            over: remapOver(select.over, alias),
            alias: select.alias
          }, _.isUndefined);
        });
      };
    })(this);
    if (!this.isAggr(scalar.expr) && !scalar.limit) {
      opt0Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt0Selects.push({
        type: "select",
        expr: scalar.expr,
        alias: "expr"
      });
      opt0From = {
        type: "join",
        kind: "left",
        left: query.from,
        right: scalar.from,
        on: scalar.where
      };
      opt0Query = {
        type: "query",
        selects: opt0Selects,
        from: opt0From,
        where: innerWhere
      };
      opt0Query = this.optimizeQuery(opt0Query);
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, "opt0"),
        from: {
          type: "subquery",
          query: opt0Query,
          alias: "opt0"
        },
        where: this.remapFields(outerWhere, fields, scalar, "opt0"),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, "opt0")
            });
          };
        })(this))
      });
      return outerQuery;
    } else if (!scalar.limit) {
      opt0Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt0Selects.push({
        type: "select",
        expr: {
          type: "op",
          op: "row_number",
          exprs: []
        },
        over: {},
        alias: "rn"
      });
      opt0Query = {
        type: "query",
        selects: opt0Selects,
        from: query.from,
        where: innerWhere
      };
      opt0Query = this.optimizeQuery(opt0Query);
      opt1Selects = [
        {
          type: "select",
          expr: {
            type: "field",
            tableAlias: "opt0",
            column: "rn"
          },
          alias: "rn"
        }
      ];
      opt1Selects = opt1Selects.concat(_.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: "opt0",
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this)));
      opt1Selects.push({
        type: "select",
        expr: this.remapFields(scalar.expr, fields, null, "opt0"),
        alias: "expr"
      });
      opt1From = {
        type: "join",
        kind: "left",
        left: {
          type: "subquery",
          query: opt0Query,
          alias: "opt0"
        },
        right: scalar.from,
        on: this.remapFields(scalar.where, fields, scalar, "opt0")
      };
      opt1Query = {
        type: "query",
        selects: opt1Selects,
        from: opt1From,
        groupBy: _.range(1, fields.length + 2)
      };
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, "opt1"),
        from: {
          type: "subquery",
          query: opt1Query,
          alias: "opt1"
        },
        where: this.remapFields(outerWhere, fields, scalar, "opt1"),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, "opt1")
            });
          };
        })(this))
      });
      return outerQuery;
    } else {
      opt0Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt0Selects.push({
        type: "select",
        expr: {
          type: "op",
          op: "row_number",
          exprs: []
        },
        over: {},
        alias: "rn"
      });
      opt0Query = {
        type: "query",
        selects: opt0Selects,
        from: query.from,
        where: innerWhere
      };
      opt0Query = this.optimizeQuery(opt0Query);
      opt1Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: "opt0",
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt1Selects.push({
        type: "select",
        expr: this.remapFields(scalar.expr, fields, null, "opt0"),
        alias: "expr"
      });
      opt1Selects.push({
        type: "select",
        expr: {
          type: "op",
          op: "row_number",
          exprs: []
        },
        over: {
          partitionBy: [
            {
              type: "field",
              tableAlias: "opt0",
              column: "rn"
            }
          ],
          orderBy: scalar.orderBy
        },
        alias: "rn"
      });
      opt1From = {
        type: "join",
        kind: "left",
        left: {
          type: "subquery",
          query: opt0Query,
          alias: "opt0"
        },
        right: scalar.from,
        on: this.remapFields(scalar.where, fields, scalar, "opt0")
      };
      opt1Query = {
        type: "query",
        selects: opt1Selects,
        from: opt1From
      };
      opt2Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: "opt1",
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt2Selects.push({
        type: "select",
        expr: {
          type: "field",
          tableAlias: "opt1",
          column: "expr"
        },
        alias: "expr"
      });
      opt2Query = {
        type: "query",
        selects: opt2Selects,
        from: {
          type: "subquery",
          query: opt1Query,
          alias: "opt1"
        },
        where: {
          type: "op",
          op: "=",
          exprs: [
            {
              type: "field",
              tableAlias: "opt1",
              column: "rn"
            }, {
              type: "literal",
              value: 1
            }
          ]
        }
      };
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, "opt2"),
        from: {
          type: "subquery",
          query: opt2Query,
          alias: "opt2"
        },
        where: this.remapFields(outerWhere, fields, scalar, "opt2"),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, "opt2")
            });
          };
        })(this))
      });
      return outerQuery;
    }
  };

  QueryOptimizer.prototype.findScalar = function(frag) {
    var expr, j, k, l, len, len1, len2, orderBy, ref, ref1, ref2, scalar, select;
    if (!frag || !frag.type) {
      return null;
    }
    switch (frag.type) {
      case "query":
        scalar = this.findScalar(frag.where);
        if (scalar) {
          return scalar;
        }
        ref = frag.selects;
        for (j = 0, len = ref.length; j < len; j++) {
          select = ref[j];
          scalar = this.findScalar(select.expr);
          if (scalar) {
            return scalar;
          }
        }
        if (frag.orderBy) {
          ref1 = frag.orderBy;
          for (k = 0, len1 = ref1.length; k < len1; k++) {
            orderBy = ref1[k];
            scalar = this.findScalar(orderBy.expr);
            if (scalar) {
              return scalar;
            }
          }
        }
        break;
      case "scalar":
        return frag;
      case "op":
        ref2 = frag.exprs;
        for (l = 0, len2 = ref2.length; l < len2; l++) {
          expr = ref2[l];
          scalar = this.findScalar(expr);
          if (scalar) {
            return scalar;
          }
        }
    }
    return null;
  };

  QueryOptimizer.prototype.extractFromAliases = function(from) {
    switch (from.type) {
      case "table":
      case "subquery":
      case "subexpr":
        return [from.alias];
      case "join":
        return this.extractFromAliases(from.left).concat(this.extractFromAliases(from.right));
    }
    throw new Error("Unknown from type " + from.type);
  };

  QueryOptimizer.prototype.extractFields = function(frag) {
    if (!frag || !frag.type) {
      return [];
    }
    switch (frag.type) {
      case "query":
        return _.flatten(_.map(frag.selects, (function(_this) {
          return function(select) {
            return _this.extractFields(select.expr);
          };
        })(this))).concat(this.extractFields(frag.where)).concat(_.flatten(_.map(frag.orderBy, (function(_this) {
          return function(orderBy) {
            return _this.extractFields(orderBy.expr);
          };
        })(this))));
      case "field":
        return [frag];
      case "op":
        return _.flatten(_.map(frag.exprs, this.extractFields));
      case "case":
        return this.extractFields(frag.input).concat(_.flatten(_.map(frag.cases, (function(_this) {
          return function(cs) {
            return _this.extractFields(cs.when).concat(_this.extractFields(cs.then));
          };
        })(this)))).concat(this.extractFields(frag["else"]));
      case "scalar":
        return this.extractFields(frag.frag).concat(this.extractFields(frag.where)).concat(_.map(frag.orderBy, (function(_this) {
          return function(ob) {
            return _this.extractFields(ob.frag);
          };
        })(this)));
      case "literal":
        return [];
      case "token":
        return [];
      default:
        throw new Error("Unsupported extractFields with type " + frag.type);
    }
  };

  QueryOptimizer.prototype.isAggr = function(expr) {
    var ref;
    if (!expr || !expr.type) {
      return false;
    }
    switch (expr.type) {
      case "field":
        return false;
      case "op":
        return (ref = expr.op) === 'sum' || ref === 'min' || ref === 'max' || ref === 'avg' || ref === 'count' || ref === 'stdev' || ref === 'stdevp' || ref === 'var' || ref === 'varp';
      case "case":
        return _.any(expr.cases, (function(_this) {
          return function(cs) {
            return _this.isAggr(cs.then);
          };
        })(this));
      case "scalar":
        return false;
      case "literal":
        return false;
      case "token":
        return false;
      default:
        throw new Error("Unsupported isAggr with type " + expr.type);
    }
  };

  QueryOptimizer.prototype.remapFields = function(frag, fields, scalar, tableAlias) {
    var field, j, len;
    if (!frag || !frag.type) {
      return frag;
    }
    switch (frag.type) {
      case "field":
        for (j = 0, len = fields.length; j < len; j++) {
          field = fields[j];
          if (field.tableAlias === frag.tableAlias && field.column === frag.column) {
            return {
              type: "field",
              tableAlias: tableAlias,
              column: "opt_" + field.tableAlias + "_" + field.column
            };
          }
        }
        return frag;
      case "op":
        return _.extend({}, frag, {
          exprs: _.map(frag.exprs, (function(_this) {
            return function(ex) {
              return _this.remapFields(ex, fields, scalar, tableAlias);
            };
          })(this))
        });
      case "case":
        return _.extend({}, frag, {
          input: this.remapFields(frag.input, fields, scalar, tableAlias),
          cases: _.map(frag.cases, (function(_this) {
            return function(cs) {
              return {
                when: _this.remapFields(cs.when, fields, scalar, tableAlias),
                then: _this.remapFields(cs.then, fields, scalar, tableAlias)
              };
            };
          })(this)),
          "else": this.remapFields(frag["else"], fields, scalar, tableAlias)
        });
      case "scalar":
        if (scalar === frag) {
          return {
            type: "field",
            tableAlias: tableAlias,
            column: "expr"
          };
        } else {
          return _.extend({}, frag, {
            frag: this.remapFields(frag.frag, fields, scalar, tableAlias),
            from: this.remapFields(frag.from, fields, scalar, tableAlias),
            where: this.remapFields(frag.where, fields, scalar, tableAlias),
            orderBy: this.remapFields(frag.orderBy, fields, scalar, tableAlias)
          });
        }
        break;
      case "table":
        return frag;
      case "join":
        return _.extend({}, frag, {
          left: this.remapFields(frag.left, fields, scalar, tableAlias),
          right: this.remapFields(frag.right, fields, scalar, tableAlias),
          on: this.remapFields(frag.on, fields, scalar, tableAlias)
        });
      case "literal":
        return frag;
      case "token":
        return frag;
      default:
        throw new Error("Unsupported remapFields with type " + frag.type);
    }
  };

  return QueryOptimizer;

})();
