"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var SchemaMap, SqlFragment;
SqlFragment = require('./SqlFragment'); // Maps tables and columns to a secure sql fragment. Base class is simple passthrough

module.exports = SchemaMap = /*#__PURE__*/function () {
  function SchemaMap() {
    (0, _classCallCheck2["default"])(this, SchemaMap);
  }

  (0, _createClass2["default"])(SchemaMap, [{
    key: "mapTable",
    // Maps a table to a secured, sanitized version
    value: function mapTable(table) {
      return new SqlFragment(table);
    } // Map a column reference of a table aliased as escaped alias alias

  }, {
    key: "mapColumn",
    value: function mapColumn(table, column, alias) {
      return new SqlFragment(alias + "." + column);
    } // Escapes a table alias. Should prefix with alias_ or similar for security

  }, {
    key: "mapTableAlias",
    value: function mapTableAlias(alias) {
      return alias;
    }
  }]);
  return SchemaMap;
}();