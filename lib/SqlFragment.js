var SqlFragment, _, pgescape;

_ = require('lodash');

pgescape = require('pg-escape');

module.exports = SqlFragment = (function() {
  function SqlFragment(sql, params) {
    this.sql = sql || "";
    this.params = params || [];
  }

  SqlFragment.prototype.append = function(val, params) {
    if (_.isString(val)) {
      this.sql += val;
      this.params = this.params.concat(params || []);
    } else {
      this.sql += val.sql;
      this.params = this.params.concat(val.params);
    }
    return this;
  };

  SqlFragment.prototype.isEmpty = function() {
    return this.sql.length === 0;
  };

  SqlFragment.join = function(list, joiner) {
    return new SqlFragment(_.map(list, function(fr) {
      return fr.sql;
    }).join(joiner), [].concat.apply([], _.pluck(list, "params")));
  };

  SqlFragment.prototype.toInline = function() {
    var n, sql;
    n = 0;
    sql = this.sql.replace(/\?/g, (function(_this) {
      return function(str) {
        var param;
        param = _this.params[n];
        n += 1;
        if (param === null) {
          return "null";
        }
        if (typeof param === "string") {
          return pgescape.literal(param);
        }
        if (typeof param === "number") {
          return "" + param;
        }
        if (_.isArray(param)) {
          return "array[" + _.map(param, function(p) {
            return pgescape.literal(p);
          }).join(',') + "]";
        }
        if (typeof param === "object") {
          return "(" + pgescape.literal(JSON.stringify(param)) + "::json)";
        }
        throw new Error("Unsupported parameter: " + param);
      };
    })(this));
    return sql;
  };

  return SqlFragment;

})();
