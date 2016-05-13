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
    this.aliasNum = 0;
  }

  QueryOptimizer.prototype.debugQuery = function(query) {
    var JsonqlCompiler, SchemaMap, ex, sql;
    SchemaMap = require('./SchemaMap');
    JsonqlCompiler = require('./JsonqlCompiler');
    try {
      sql = new JsonqlCompiler(new SchemaMap(), false).compileQuery(query);
      console.log("===== SQL ======");
      console.log(sql.toInline());
      return console.log("================");
    } catch (_error) {
      ex = _error;
      console.log("FAILURE: " + ex.message);
      return console.log(JSON.stringify(query, null, 2));
    }
  };

  QueryOptimizer.prototype.optimizeQuery = function(query, debug) {
    var i, j, optQuery;
    if (debug == null) {
      debug = false;
    }
    if (debug) {
      console.log("================== BEFORE OPT ================");
      this.debugQuery(query);
    }
    for (i = j = 0; j < 20; i = ++j) {
      optQuery = this.rewriteScalar(query);
      if (_.isEqual(optQuery, query)) {
        return optQuery;
      }
      if (debug) {
        console.log("================== OPT " + i + " ================");
        this.debugQuery(optQuery);
      }
      query = optQuery;
    }
    throw new Error("Unable to optimize query (infinite loop): " + (JSON.stringify(query)));
  };

  QueryOptimizer.prototype.rewriteScalar = function(query) {
    var fields, fromAliases, innerWhere, newScalarAlias, oldScalarAlias, opt1Alias, opt1From, opt1Query, opt1Selects, opt2Alias, opt2From, opt2Query, opt2Selects, opt3Alias, opt3Query, opt3Selects, outerQuery, outerWhere, remapOver, remapSelects, scalar, wheres;
    query = this.optimizeInnerQueries(query);
    scalar = this.findScalar(query);
    if (!scalar) {
      return query;
    }
    if (!scalar.from.alias) {
      return query;
    }
    oldScalarAlias = scalar.from.alias;
    newScalarAlias = this.createAlias();
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
      opt1Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt1Selects.push({
        type: "select",
        expr: this.changeAlias(scalar.expr, oldScalarAlias, newScalarAlias),
        alias: "expr"
      });
      opt1From = {
        type: "join",
        kind: "left",
        left: query.from,
        right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
        on: this.changeAlias(scalar.where, oldScalarAlias, newScalarAlias)
      };
      opt1Query = {
        type: "query",
        selects: opt1Selects,
        from: opt1From,
        where: innerWhere
      };
      opt1Query = this.optimizeQuery(opt1Query, false);
      opt1Alias = this.createAlias();
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, opt1Alias),
        from: {
          type: "subquery",
          query: opt1Query,
          alias: opt1Alias
        },
        where: this.remapFields(outerWhere, fields, scalar, opt1Alias),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, opt1Alias)
            });
          };
        })(this))
      });
      return outerQuery;
    } else if (!scalar.limit) {
      opt1Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt1Selects.push({
        type: "select",
        expr: {
          type: "op",
          op: "row_number",
          exprs: []
        },
        over: {},
        alias: "rn"
      });
      opt1Alias = this.createAlias();
      opt1Query = {
        type: "query",
        selects: opt1Selects,
        from: query.from,
        where: innerWhere
      };
      opt1Query = this.optimizeQuery(opt1Query, false);
      opt2Selects = [
        {
          type: "select",
          expr: {
            type: "field",
            tableAlias: opt1Alias,
            column: "rn"
          },
          alias: "rn"
        }
      ];
      opt2Selects = opt2Selects.concat(_.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: opt1Alias,
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this)));
      opt2Selects.push({
        type: "select",
        expr: this.changeAlias(this.remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias),
        alias: "expr"
      });
      opt2From = {
        type: "join",
        kind: "left",
        left: {
          type: "subquery",
          query: opt1Query,
          alias: opt1Alias
        },
        right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
        on: this.changeAlias(this.remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
      };
      opt2Query = {
        type: "query",
        selects: opt2Selects,
        from: opt2From,
        groupBy: _.range(1, fields.length + 2)
      };
      opt2Alias = this.createAlias();
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, opt2Alias),
        from: {
          type: "subquery",
          query: opt2Query,
          alias: opt2Alias
        },
        where: this.remapFields(outerWhere, fields, scalar, opt2Alias),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, opt2Alias)
            });
          };
        })(this))
      });
      return outerQuery;
    } else {
      opt1Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: field,
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt1Selects.push({
        type: "select",
        expr: {
          type: "op",
          op: "row_number",
          exprs: []
        },
        over: {},
        alias: "rn"
      });
      opt1Query = {
        type: "query",
        selects: opt1Selects,
        from: query.from,
        where: innerWhere
      };
      opt1Query = this.optimizeQuery(opt1Query, false);
      opt1Alias = this.createAlias();
      opt2Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: opt1Alias,
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt2Selects.push({
        type: "select",
        expr: this.changeAlias(this.remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias),
        alias: "expr"
      });
      opt2Selects.push({
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
              tableAlias: opt1Alias,
              column: "rn"
            }
          ],
          orderBy: _.map(scalar.orderBy, (function(_this) {
            return function(ob) {
              if (ob.expr) {
                return _.extend({}, ob, {
                  expr: _this.changeAlias(ob.expr, oldScalarAlias, newScalarAlias)
                });
              }
              return ob;
            };
          })(this))
        },
        alias: "rn"
      });
      opt2From = {
        type: "join",
        kind: "left",
        left: {
          type: "subquery",
          query: opt1Query,
          alias: opt1Alias
        },
        right: this.changeAlias(scalar.from, oldScalarAlias, newScalarAlias),
        on: this.changeAlias(this.remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
      };
      opt2Query = {
        type: "query",
        selects: opt2Selects,
        from: opt2From
      };
      opt2Alias = this.createAlias();
      opt3Selects = _.map(fields, (function(_this) {
        return function(field) {
          return {
            type: "select",
            expr: {
              type: "field",
              tableAlias: opt2Alias,
              column: "opt_" + field.tableAlias + "_" + field.column
            },
            alias: "opt_" + field.tableAlias + "_" + field.column
          };
        };
      })(this));
      opt3Selects.push({
        type: "select",
        expr: {
          type: "field",
          tableAlias: opt2Alias,
          column: "expr"
        },
        alias: "expr"
      });
      opt3Query = {
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
            {
              type: "field",
              tableAlias: opt2Alias,
              column: "rn"
            }, {
              type: "literal",
              value: 1
            }
          ]
        }
      };
      opt3Alias = this.createAlias();
      outerQuery = _.extend({}, query, {
        selects: remapSelects(query.selects, opt3Alias),
        from: {
          type: "subquery",
          query: opt3Query,
          alias: opt3Alias
        },
        where: this.remapFields(outerWhere, fields, scalar, opt3Alias),
        orderBy: _.map(query.orderBy, (function(_this) {
          return function(orderBy) {
            if (!orderBy.expr) {
              return orderBy;
            }
            return _.extend({}, orderBy, {
              expr: _this.remapFields(orderBy.expr, fields, scalar, opt3Alias)
            });
          };
        })(this))
      });
      return outerQuery;
    }
  };

  QueryOptimizer.prototype.optimizeInnerQueries = function(query) {
    var optimizeFrom;
    optimizeFrom = (function(_this) {
      return function(from) {
        switch (from.type) {
          case "table":
          case "subexpr":
            return from;
          case "join":
            return _.extend({}, from, {
              left: optimizeFrom(from.left),
              right: optimizeFrom(from.right)
            });
          case "subquery":
            return _.extend({}, from, {
              query: _this.optimizeQuery(from.query)
            });
          default:
            throw new Error("Unknown optimizeFrom type " + from.type);
        }
      };
    })(this);
    return query = _.extend({}, query, {
      from: optimizeFrom(query.from)
    });
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

  QueryOptimizer.prototype.changeAlias = function(frag, fromAlias, toAlias) {
    var newFrag;
    if (!frag || !frag.type) {
      return frag;
    }
    switch (frag.type) {
      case "field":
        if (frag.tableAlias === fromAlias) {
          return {
            type: "field",
            tableAlias: toAlias,
            column: frag.column
          };
        }
        return frag;
      case "op":
        return _.extend({}, frag, {
          exprs: _.map(frag.exprs, (function(_this) {
            return function(ex) {
              return _this.changeAlias(ex, fromAlias, toAlias);
            };
          })(this))
        });
      case "case":
        return _.extend({}, frag, {
          input: this.changeAlias(frag.input, fromAlias, toAlias),
          cases: _.map(frag.cases, (function(_this) {
            return function(cs) {
              return {
                when: _this.changeAlias(cs.when, fromAlias, toAlias),
                then: _this.changeAlias(cs.then, fromAlias, toAlias)
              };
            };
          })(this)),
          "else": this.changeAlias(frag["else"], fromAlias, toAlias)
        });
      case "scalar":
        newFrag = _.extend({}, frag, {
          expr: this.changeAlias(frag.expr, fromAlias, toAlias),
          from: this.changeAlias(frag.from, fromAlias, toAlias),
          where: this.changeAlias(frag.where, fromAlias, toAlias),
          orderBy: this.changeAlias(frag.orderBy, fromAlias, toAlias)
        });
        if (frag.orderBy) {
          newFrag.orderBy = _.map(frag.orderBy, (function(_this) {
            return function(ob) {
              if (ob.expr) {
                return _.extend({}, ob, {
                  expr: _this.changeAlias(ob.expr, fromAlias, toAlias)
                });
              }
              return ob;
            };
          })(this));
        }
        return newFrag;
      case "table":
        if (frag.alias === fromAlias) {
          return {
            type: "table",
            table: frag.table,
            alias: toAlias
          };
        }
        return frag;
      case "join":
        return _.extend({}, frag, {
          left: this.changeAlias(frag.left, fromAlias, toAlias),
          right: this.changeAlias(frag.right, fromAlias, toAlias),
          on: this.changeAlias(frag.on, fromAlias, toAlias)
        });
      case "literal":
        return frag;
      case "token":
        return frag;
      default:
        throw new Error("Unsupported changeAlias with type " + frag.type);
    }
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
    var field, j, len, newFrag;
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
          newFrag = _.extend({}, frag, {
            expr: this.remapFields(frag.expr, fields, scalar, tableAlias),
            from: this.remapFields(frag.from, fields, scalar, tableAlias),
            where: this.remapFields(frag.where, fields, scalar, tableAlias)
          });
          if (frag.orderBy) {
            newFrag.orderBy = _.map(frag.orderBy, (function(_this) {
              return function(ob) {
                if (ob.expr) {
                  return _.extend({}, ob, {
                    expr: _this.remapFields(ob.expr, fields, scalar, tableAlias)
                  });
                }
                return ob;
              };
            })(this));
          }
          return newFrag;
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

  QueryOptimizer.prototype.createAlias = function() {
    var alias;
    alias = "opt" + this.aliasNum;
    this.aliasNum += 1;
    return alias;
  };

  return QueryOptimizer;

})();
