var QueryOptimizer, _,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');


/*

Scalar subqueries can be very slow in Postgresql as they are not re-written but instead loop over and over.

This attempts to re-write them as left outer joins, which is a complex tranformation
 */

module.exports = QueryOptimizer = (function() {
  function QueryOptimizer() {
    this.isAggr = bind(this.isAggr, this);
    this.extractFields = bind(this.extractFields, this);
  }

  QueryOptimizer.prototype.optimizeQuery = function(query) {
    var fields, fromAliases, opt0From, opt0Query, opt0Selects, opt1From, opt1Query, opt1Selects, opt2Query, opt2Selects, outerQuery, scalar;
    if (query.having) {
      return query;
    }
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
        where: query.where
      };
      outerQuery = {
        type: "query",
        selects: _.map(query.selects, (function(_this) {
          return function(select) {
            return {
              type: "select",
              expr: _this.remapFields(select.expr, fields, scalar, "opt0"),
              alias: select.alias
            };
          };
        })(this)),
        from: {
          type: "subquery",
          query: opt0Query,
          alias: "opt0"
        },
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
      };
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
        where: query.where
      };
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
      outerQuery = {
        type: "query",
        selects: _.map(query.selects, (function(_this) {
          return function(select) {
            return {
              type: "select",
              expr: _this.remapFields(select.expr, fields, scalar, "opt1"),
              alias: select.alias
            };
          };
        })(this)),
        from: {
          type: "subquery",
          query: opt1Query,
          alias: "opt1"
        },
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
      };
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
        where: query.where
      };
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
      outerQuery = {
        type: "query",
        selects: _.map(query.selects, (function(_this) {
          return function(select) {
            return {
              type: "select",
              expr: _this.remapFields(select.expr, fields, scalar, "opt2"),
              alias: select.alias
            };
          };
        })(this)),
        from: {
          type: "subquery",
          query: opt2Query,
          alias: "opt2"
        },
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
      };
      return outerQuery;
    }
  };

  QueryOptimizer.prototype.findScalar = function(frag) {
    var expr, i, j, k, len, len1, len2, orderBy, ref, ref1, ref2, scalar, select;
    if (!frag) {
      return null;
    }
    switch (frag.type) {
      case "query":
        scalar = this.findScalar(frag.where);
        if (scalar) {
          return scalar;
        }
        ref = frag.selects;
        for (i = 0, len = ref.length; i < len; i++) {
          select = ref[i];
          scalar = this.findScalar(select.expr);
          if (scalar) {
            return scalar;
          }
        }
        if (frag.orderBy) {
          ref1 = frag.orderBy;
          for (j = 0, len1 = ref1.length; j < len1; j++) {
            orderBy = ref1[j];
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
        for (k = 0, len2 = ref2.length; k < len2; k++) {
          expr = ref2[k];
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
      default:
        throw new Error("Unsupported isAggr with type " + expr.type);
    }
  };

  QueryOptimizer.prototype.remapFields = function(expr, fields, scalar, tableAlias) {
    var field, i, len;
    if (!expr || !expr.type) {
      return expr;
    }
    switch (expr.type) {
      case "field":
        for (i = 0, len = fields.length; i < len; i++) {
          field = fields[i];
          if (field === expr) {
            return {
              type: "field",
              tableAlias: tableAlias,
              column: "opt_" + field.tableAlias + "_" + field.column
            };
          }
        }
        return expr;
      case "op":
        return _.extend({}, expr, {
          exprs: _.map(expr.exprs, (function(_this) {
            return function(ex) {
              return _this.remapFields(ex, fields, scalar, tableAlias);
            };
          })(this))
        });
      case "case":
        return _.extend({}, expr, {
          input: this.remapFields(expr.input, fields, scalar, tableAlias),
          cases: _.map(expr.cases, (function(_this) {
            return function(cs) {
              return {
                when: _this.remapFields(cs.when, fields, scalar, tableAlias),
                then: _this.remapFields(cs.then, fields, scalar, tableAlias)
              };
            };
          })(this)),
          "else": this.remapFields(expr["else"], fields, scalar, tableAlias)
        });
      case "scalar":
        if (scalar === expr) {
          return {
            type: "field",
            tableAlias: tableAlias,
            column: "expr"
          };
        }
        return expr;
      case "literal":
        return expr;
      default:
        throw new Error("Unsupported remapFields with type " + expr.type);
    }
  };

  return QueryOptimizer;

})();
