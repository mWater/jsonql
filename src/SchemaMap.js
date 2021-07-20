SqlFragment = require './SqlFragment'

# Maps tables and columns to a secure sql fragment. Base class is simple passthrough
module.exports = class SchemaMap
  # Maps a table to a secured, sanitized version
  mapTable: (table) -> 
    return new SqlFragment(table)

  # Map a column reference of a table aliased as escaped alias alias
  mapColumn: (table, column, alias) ->
    return new SqlFragment(alias + "." + column)

  # Escapes a table alias. Should prefix with alias_ or similar for security
  mapTableAlias: (alias) ->
    return alias

