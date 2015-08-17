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

  LookupSchemaMap.prototype.mapTable = function(tableId) {
    var table;
    table = _.find(this.schema.tables, {
      id: tableId
    });
    if (!table) {
      throw new Error("Invalid table " + tableId);
    }
    return new SqlFragment(table.sql || table.id);
  };

  LookupSchemaMap.prototype.mapColumn = function(tableId, columnId, alias) {
    var column, sql, table;
    table = _.find(this.schema.tables, {
      id: tableId
    });
    if (!table) {
      throw new Error("Invalid table " + tableId);
    }
    column = this.findColumn(table.contents, columnId);
    if (!column) {
      throw new Error("Invalid column " + columnId);
    }
    if (column.sql) {
      sql = column.sql.replace(/\{alias\}/g, alias);
    } else {
      sql = alias + "." + column.id;
    }
    return new SqlFragment(sql);
  };

  LookupSchemaMap.prototype.findColumn = function(contents, columnId) {
    var i, item, len, subitem;
    for (i = 0, len = contents.length; i < len; i++) {
      item = contents[i];
      if (item.type === "section") {
        subitem = this.findColumn(item.contents, columnId);
        if (subitem) {
          return subitem;
        }
      } else if (item.id === columnId) {
        return item;
      }
    }
  };

  LookupSchemaMap.prototype.mapTableAlias = function(alias) {
    return "alias_" + alias;
  };

  return LookupSchemaMap;

})(SchemaMap);
