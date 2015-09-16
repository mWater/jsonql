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
    var escapeLiteral, n, sql;
    escapeLiteral = function(val) {
      if (val === null) {
        return "null";
      }
      if (typeof val === "string") {
        return pgescape.literal(val);
      }
      if (typeof val === "number") {
        return "" + val;
      }
      if (_.isArray(val)) {
        return "array[" + _.map(val, escapeLiteral).join(',') + "]";
      }
      if (typeof val === "object") {
        return "(" + pgescape.literal(JSON.stringify(val)) + "::json)";
      }
      throw new Error("Unsupported literal value: " + val);
    };
    n = 0;
    sql = this.sql.replace(/\?/g, (function(_this) {
      return function(str) {
        var param;
        param = _this.params[n];
        n += 1;
        return escapeLiteral(param);
      };
    })(this));
    return sql;
  };

  return SqlFragment;

})();
