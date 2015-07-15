_ = require 'lodash'
SqlFragment = require './SqlFragment'
SchemaMap = require './SchemaMap'
 
# Schema map for direct name mapping
module.exports = class LookupSchemaMap extends SchemaMap
  constructor: (schema) ->
    @schema = schema
 
  # Map a table
  mapTable: (table) -> 
    tableExpr = _.find(@schema.tables, {id: table})
    if !tableExpr
      throw new Error("Invalid table #{table}")
    return new SqlFragment(tableExpr.sql)
 
  # Map a column reference of a table aliased as escaped {alias}
  mapColumn: (table, column, alias) ->
    tableExpr = _.find(@schema.tables, {id: table})
    if !tableExpr
      throw new Error("Invalid table #{table}")
    columnExpr = _.find(tableExpr.columns, {id: column})
    if !columnExpr
      throw new Error("Invalid column #{column}")    
    sql = columnExpr.sql.replace(/\{alias\}/g, alias)
    return new SqlFragment(sql)
 
  # Escapes a table alias. Should prefix with alias_ or similar for security
  mapTableAlias: (alias) ->
    return "alias_" + alias
