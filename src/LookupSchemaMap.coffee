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
  mapColumn: (tableId, columnId, alias) ->
    table = _.find(@schema.tables, {id: tableId})
    if !table
      throw new Error("Invalid table #{tableId}")

    column = @findColumn(table.contents, columnId)
    if !column
      throw new Error("Invalid column #{columnId}")    

    # Get sql
    if column.sql
      sql = column.sql.replace(/\{alias\}/g, alias)
    else
      sql = "#{alias}.#{column.id}"

    return new SqlFragment(sql)

  # Find a column in a table or section
  findColumn: (contents, columnId) ->
    for item in contents
      if item.type == "section"
        subitem = @findColumn(item.contents, columnId)
        if subitem
          return subitem
      else if item.id == columnId
        return item
  # Escapes a table alias. Should prefix with alias_ or similar for security
  mapTableAlias: (alias) ->
    return "alias_" + alias
