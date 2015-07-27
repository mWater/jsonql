var JsonqlCompiler, SqlFragment, _, isInt,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

SqlFragment = require('./SqlFragment');

module.exports = JsonqlCompiler = (function() {
  function JsonqlCompiler(schemaMap) {
    this.schemaMap = schemaMap;
    this.nextId = 1;
  }

  JsonqlCompiler.prototype.compileQuery = function(query, aliases, ctes) {
    var f, fields, frag, from, i, len, ref, selects, w, where, withClauses;
    if (aliases == null) {
      aliases = {};
    }
    if (ctes == null) {
      ctes = {};
    }
    frag = new SqlFragment();
    aliases = _.clone(aliases);
    ctes = _.clone(ctes);
    if (query.withs && query.withs.length > 0) {
      withClauses = [];
      ref = query.withs;
      for (i = 0, len = ref.length; i < len; i++) {
        w = ref[i];
        f = new SqlFragment('"').append(this.schemaMap.mapTableAlias(w.alias));
        f.append("\" as (");
        f.append(this.compileQuery(w.query, aliases));
        f.append(")");
        withClauses.push(f);
        if (ctes[w.alias]) {
          throw new Error("CTE alias " + w.alias + " in use");
        }
        fields = _.map(w.query.selects, function(s) {
          return s.alias;
        });
        ctes[w.alias] = fields;
      }
      frag.append("with ");
      frag.append(SqlFragment.join(withClauses, ", "));
      frag.append(" ");
    }
    frag.append('select ');
    from = this.compileFrom(query.from, aliases, ctes);
    selects = _.map(query.selects, (function(_this) {
      return function(s) {
        return _this.compileSelect(s, aliases);
      };
    })(this));
    frag.append(SqlFragment.join(selects, ", "));
    frag.append(" from ");
    frag.append(from);
    if (query.where) {
      where = this.compileExpr(query.where, aliases);
      if (!where.isEmpty()) {
        frag.append(" where ");
        frag.append(where);
      }
    }
    if (query.groupBy) {
      if (!_.isArray(query.groupBy) || !_.all(query.groupBy, isInt)) {
        throw new Error("Invalid groupBy");
      }
      if (query.groupBy.length > 0) {
        frag.append(" group by ").append(SqlFragment.join(_.map(query.groupBy, function(g) {
          return new SqlFragment("" + g);
        }), ", "));
      }
    }
    if (query.orderBy) {
      frag.append(this.compileOrderBy(query.orderBy, aliases));
    }
    if (query.limit != null) {
      if (!isInt(query.limit)) {
        throw new Error("Invalid limit");
      }
      frag.append(" limit ").append(new SqlFragment("?", [query.limit]));
    }
    if (query.offset != null) {
      if (!isInt(query.offset)) {
        throw new Error("Invalid offset");
      }
      frag.append(" offset ").append(new SqlFragment("?", [query.offset]));
    }
    return frag;
  };

  JsonqlCompiler.prototype.compileSelect = function(select, aliases) {
    var frag;
    frag = this.compileExpr(select.expr, aliases);
    if (select.over) {
      frag.append(" over (");
      if (select.over.partitionBy) {
        frag.append("partition by ");
        frag.append(SqlFragment.join(_.map(select.over.partitionBy, (function(_this) {
          return function(pb) {
            return _this.compileExpr(pb, aliases);
          };
        })(this)), ", "));
      }
      frag.append(")");
    }
    frag.append(" as ");
    this.validateAlias(select.alias);
    frag.append('"' + select.alias + '"');
    return frag;
  };

  JsonqlCompiler.prototype.compileFrom = function(from, aliases, ctes) {
    var fields, left, onSql, ref, right, subquery;
    if (aliases == null) {
      aliases = {};
    }
    if (ctes == null) {
      ctes = {};
    }
    switch (from.type) {
      case "table":
        this.validateAlias(from.alias);
        if (aliases[from.alias] != null) {
          throw new Error("Alias " + from.alias + " in use");
        }
        if (ctes[from.table]) {
          aliases[from.alias] = ctes[from.table];
          return new SqlFragment(this.schemaMap.mapTableAlias(from.table)).append(' as "').append(this.schemaMap.mapTableAlias(from.alias)).append('"');
        }
        aliases[from.alias] = from.table;
        return this.schemaMap.mapTable(from.table).append(new SqlFragment(' as "' + this.schemaMap.mapTableAlias(from.alias) + '"'));
      case "join":
        left = this.compileFrom(from.left, aliases);
        right = this.compileFrom(from.right, aliases);
        if (_.intersection(_.keys(left.aliases), _.keys(right.aliases)).length > 0) {
          throw new Error("Duplicate aliases");
        }
        _.extend(aliases, left.aliases);
        _.extend(aliases, right.aliases);
        onSql = this.compileExpr(from.on, aliases);
        if ((ref = from.kind) !== 'inner' && ref !== 'left' && ref !== 'right') {
          throw new Error("Unsupported join kind " + from.kind);
        }
        return new SqlFragment("(").append(left.sql).append(" " + from.kind + " join ").append(right.sql).append(" on ").append(onSql).append(")");
      case "subquery":
        this.validateAlias(from.alias);
        if (aliases[from.alias] != null) {
          throw new Error("Alias " + from.alias + " in use");
        }
        subquery = this.compileQuery(from.query, aliases, ctes);
        fields = _.map(from.query.selects, function(s) {
          return s.alias;
        });
        aliases[from.alias] = fields;
        return new SqlFragment("(").append(subquery).append(') as "').append(this.schemaMap.mapTableAlias(from.alias)).append('"');
      default:
        throw new Error("Unsupported type " + from.type);
    }
  };

  JsonqlCompiler.prototype.compileOrderBy = function(orderBy, aliases) {
    var frag;
    frag = new SqlFragment();
    if (!_.isArray(orderBy)) {
      throw new Error("Invalid orderBy");
    }
    if (!_.all(orderBy, (function(_this) {
      return function(o) {
        var ref;
        if (!isInt(o.ordinal) && !o.expr) {
          return false;
        }
        return (o.direction == null) || ((ref = o.direction) === 'asc' || ref === 'desc');
      };
    })(this))) {
      throw new Error("Invalid orderBy");
    }
    if (orderBy.length > 0) {
      frag.append(" order by ").append(SqlFragment.join(_.map(orderBy, (function(_this) {
        return function(o) {
          var f;
          if (_.isNumber(o.ordinal)) {
            f = new SqlFragment("" + o.ordinal);
          } else {
            f = _this.compileExpr(o.expr, aliases);
          }
          if (o.direction) {
            f.append(" " + o.direction);
          }
          return f;
        };
      })(this)), ", "));
    }
    return frag;
  };

  JsonqlCompiler.prototype.compileExpr = function(expr, aliases) {
    var ref, ref1, ref2;
    if (aliases == null) {
      throw new Error("Missing aliases");
    }
    if (expr == null) {
      return new SqlFragment("null");
    }
    if ((ref = typeof expr) === "number" || ref === "string" || ref === "boolean") {
      return new SqlFragment("?", [expr]);
    }
    switch (expr.type) {
      case "literal":
        return new SqlFragment("?", [expr.value]);
      case "op":
        return this.compileOpExpr(expr, aliases);
      case "field":
        if (aliases[expr.tableAlias] == null) {
          throw new Error("Alias " + expr.tableAlias + " unknown");
        }
        if (_.isArray(aliases[expr.tableAlias])) {
          if (ref1 = expr.column, indexOf.call(aliases[expr.tableAlias], ref1) < 0) {
            throw new Error("Unknown column " + expr.column + " of " + expr.tableAlias);
          }
          return new SqlFragment(this.schemaMap.mapTableAlias(expr.tableAlias)).append('.').append(expr.column);
        }
        return this.schemaMap.mapColumn(aliases[expr.tableAlias], expr.column, this.schemaMap.mapTableAlias(expr.tableAlias));
      case "scalar":
        return this.compileScalar(expr, aliases);
      case "token":
        if ((ref2 = expr.token) === "!bbox!" || ref2 === "!scale_denominator!" || ref2 === "!pixel_width!" || ref2 === "!pixel_height!") {
          return new SqlFragment(expr.token);
        }
        throw new Error("Unsupported token " + expr.token);
        break;
      default:
        throw new Error("Unsupported type " + expr.type);
    }
  };

  JsonqlCompiler.prototype.compileOpExpr = function(expr, aliases) {
    var frag, functions, inner, ref, ref1;
    functions = ["avg", "min", "max", "sum", "count", "stdev", "stdevp", "var", "varp", "row_number", "left", "right", "substr"];
    switch (expr.op) {
      case ">":
      case "<":
      case ">=":
      case "<=":
      case "=":
      case "<>":
      case "+":
      case "-":
      case "*":
      case "/":
      case "~":
      case "~*":
      case "like":
      case "&&":
        frag = new SqlFragment("(").append(this.compileExpr(expr.exprs[0], aliases)).append(new SqlFragment(" " + expr.op + " "));
        if ((ref = expr.modifier) === 'any' || ref === 'all') {
          frag.append(expr.modifier).append("(").append(this.compileExpr(expr.exprs[1], aliases)).append("))");
        } else {
          frag.append(this.compileExpr(expr.exprs[1], aliases)).append(")");
        }
        return frag;
      case "and":
      case "or":
        if (expr.exprs.length === 0) {
          return new SqlFragment();
        } else if (expr.exprs.length === 1) {
          return this.compileExpr(expr.exprs[0], aliases);
        } else {
          inner = SqlFragment.join(_.map(expr.exprs, (function(_this) {
            return function(e) {
              return _this.compileExpr(e, aliases);
            };
          })(this)), " " + expr.op + " ");
          return new SqlFragment("(").append(inner).append(")");
        }
        break;
      case "is null":
      case "is not null":
        return new SqlFragment("(").append(this.compileExpr(expr.exprs[0], aliases)).append(new SqlFragment(" " + expr.op)).append(")");
      case "not":
        return new SqlFragment("(not ").append(this.compileExpr(expr.exprs[0], aliases)).append(")");
      case "::text":
      case "::geometry":
      case "::geography":
      case "::uuid":
      case "::integer":
      case "::decimal":
        return new SqlFragment("(").append(this.compileExpr(expr.exprs[0], aliases)).append(expr.op).append(")");
      default:
        if ((ref1 = expr.op, indexOf.call(functions, ref1) >= 0) || expr.op.match(/^ST_[a-zA-z]+$/)) {
          inner = SqlFragment.join(_.map(expr.exprs, (function(_this) {
            return function(e) {
              return _this.compileExpr(e, aliases);
            };
          })(this)), ", ");
          if (expr.op === "count" && inner.isEmpty()) {
            inner = "*";
          }
          return new SqlFragment(expr.op + "(").append(inner).append(")");
        }
        throw new Error("Unsupported op " + expr.op);
    }
  };

  JsonqlCompiler.prototype.compileScalar = function(query, aliases) {
    var frag, from, where;
    frag = new SqlFragment('(select ');
    aliases = _.clone(aliases);
    from = this.compileFrom(query.from, aliases);
    frag.append(this.compileExpr(query.expr, aliases));
    frag.append(" from ");
    frag.append(from);
    if (query.where) {
      where = this.compileExpr(query.where, aliases);
      if (!where.isEmpty()) {
        frag.append(" where ");
        frag.append(where);
      }
    }
    if (query.orderBy) {
      frag.append(this.compileOrderBy(query.orderBy, aliases));
    }
    if (query.limit != null) {
      if (!isInt(query.limit)) {
        throw new Error("Invalid limit");
      }
      frag.append(" limit ").append(new SqlFragment("?", [query.limit]));
    }
    if (query.offset != null) {
      if (!isInt(query.offset)) {
        throw new Error("Invalid offset");
      }
      frag.append(" offset ").append(new SqlFragment("?", [query.offset]));
    }
    frag.append(")");
    return frag;
  };

  JsonqlCompiler.prototype.validateAlias = function(alias) {
    if (!alias.match(/^[a-zA-Z][a-zA-Z_0-9]*$/)) {
      throw new Error("Invalid alias " + alias);
    }
  };

  return JsonqlCompiler;

})();

isInt = function(x) {
  return typeof x === 'number' && (x % 1) === 0;
};
