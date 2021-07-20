import SqlFragment from "./SqlFragment"

// Maps tables and columns to a secure sql fragment. Base class is simple passthrough
export default class SchemaMap {
  // Maps a table to a secured, sanitized version
  mapTable(table: any) {
    return new SqlFragment(table)
  }

  // Map a column reference of a table aliased as escaped alias alias
  mapColumn(table: any, column: any, alias: any) {
    return new SqlFragment(alias + "." + column)
  }

  // Escapes a table alias. Should prefix with alias_ or similar for security
  mapTableAlias(alias: any) {
    return alias
  }
}
