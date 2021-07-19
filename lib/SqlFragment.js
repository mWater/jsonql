"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var SqlFragment, _, escapeString;

_ = require('lodash'); // Fragment of SQL that has sql (text) and params (array)

module.exports = SqlFragment = /*#__PURE__*/function () {
  function SqlFragment(sql, params) {
    (0, _classCallCheck2["default"])(this, SqlFragment);
    this.sql = sql || "";
    this.params = params || [];
  } // Append a string (just sql), [sql, params], SqlFragment or plain object (has sql and params)


  (0, _createClass2["default"])(SqlFragment, [{
    key: "append",
    value: function append(val, params) {
      if (_.isString(val)) {
        this.sql += val;
        this.params = this.params.concat(params || []);
      } else {
        this.sql += val.sql;
        this.params = this.params.concat(val.params);
      }

      return this;
    }
  }, {
    key: "isEmpty",
    value: function isEmpty() {
      return this.sql.length === 0;
    }
  }, {
    key: "toInline",
    // Make into sql with parameters inlined
    value: function toInline() {
      var _this = this;

      var _escapeLiteral, n, sql; // Escapes a literal value


      _escapeLiteral = function escapeLiteral(val) {
        if (val === null) {
          return "null";
        }

        if (typeof val === "string") {
          return escapeString(val);
        }

        if (typeof val === "number") {
          return "" + val;
        }

        if (typeof val === "boolean") {
          if (val) {
            return "TRUE";
          } else {
            return "FALSE";
          }
        }

        if (_.isArray(val)) {
          return "array[" + _.map(val, _escapeLiteral).join(',') + "]";
        }

        if (val instanceof Date) {
          return escapeString(val.toISOString());
        }

        if ((0, _typeof2["default"])(val) === "object") {
          return "(" + escapeString(JSON.stringify(val)) + "::json)";
        }

        throw new Error("Unsupported literal value: " + val);
      }; // Substitute parameters


      n = 0; // All the question marks not followed by | or &
      // ?| and ?& are jsonb operators (so is ?, but it can be replaced by one of the others)

      sql = this.sql.replace(/\?(?!\||&)/g, function (str) {
        var param; // Insert nth parameter
        // Check type

        param = _this.params[n];
        n += 1;
        return _escapeLiteral(param);
      });
      return sql;
    }
  }], [{
    key: "join",
    value: function join(list, joiner) {
      return new SqlFragment(_.map(list, function (fr) {
        return fr.sql;
      }).join(joiner), [].concat.apply([], _.pluck(list, "params")));
    }
  }]);
  return SqlFragment;
}();

escapeString = function escapeString(val) {
  var backslash, prefix;
  backslash = ~val.indexOf('\\');
  prefix = backslash ? 'E' : '';
  val = val.replace(/'/g, '\'\'');
  val = val.replace(/\\/g, '\\\\');
  return prefix + '\'' + val + '\'';
};