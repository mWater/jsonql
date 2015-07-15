var LookupSchemaMap, SchemaMap, SqlFragment, _,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

_ = require('lodash');

SqlFragment = require('./SqlFragment');

SchemaMap = require('./SchemaMap');

module.exports = LookupSchemaMap = (function(superClass) {
  extend(LookupSchemaMap, superClass);

  function LookupSchemaMap(schema) {
    this.schema = schema;
  }

  LookupSchemaMap.prototype.mapTable = function(table) {
    var tableExpr;
    tableExpr = _.find(this.schema.tables, {
      id: table
    });
    if (!tableExpr) {
      throw new Error("Invalid table " + table);
    }
    return new SqlFragment(tableExpr.sql);
  };

  LookupSchemaMap.prototype.mapColumn = function(table, column, alias) {
    var columnExpr, sql, tableExpr;
    tableExpr = _.find(this.schema.tables, {
      id: table
    });
    if (!tableExpr) {
      throw new Error("Invalid table " + table);
    }
    columnExpr = _.find(tableExpr.columns, {
      id: column
    });
    if (!columnExpr) {
      throw new Error("Invalid column " + column);
    }
    sql = columnExpr.sql.replace(/\{alias\}/g, alias);
    return new SqlFragment(sql);
  };

  LookupSchemaMap.prototype.mapTableAlias = function(alias) {
    return "alias_" + alias;
  };

  return LookupSchemaMap;

})(SchemaMap);
