import SchemaMap from "./SchemaMap"
import SqlFragment from "./SqlFragment"
import { JsonQLQuery, JsonQLExpr } from "."

/** Compiles jsonql to sql */
export default class JsonqlCompiler {
  constructor(schemaMap: SchemaMap, optimizeQueries?: boolean)

  /* Compile a query (or union of queries) made up of selects, from, where, order, limit, skip
   * `aliases` are aliases to tables which have a particular row already selected
   * for example, a subquery can use a value from a parent table (parent_table.some_column) as a scalar
   * expression, so it already has a row selected.
   * ctes are aliases for common table expressions. They are a map of alias to true
   */
  compileQuery(
    query: JsonQLQuery,
    aliases?: { [alias: string]: string },
    ctes?: { [alias: string]: boolean }
  ): SqlFragment

  /** Compiles an expression
   aliases are dict of unmapped alias to table name, or true whitelisted tables (CTEs and subqueries and subexpressions)
   */
  compileExpr(expr: JsonQLExpr, aliases: { [alias: string]: string }, ctes?: { [alias: string]: boolean }): SqlFragment
}
