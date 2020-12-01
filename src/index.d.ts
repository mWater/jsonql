export { default as SqlFragment } from './SqlFragment'
export { default as JsonqlCompiler } from './JsonqlCompiler'
export { default as SchemaMap } from './SchemaMap'
// export { default as QueryOptimizer } from './QueryOptimizer'

export interface JsonQL {
  type: string
  [other: string]: any
}

export interface JsonQLQuery {
  type: "query"
  selects: JsonQLSelect[]
  from: JsonQLFrom
  where?: JsonQLExpr
  /** groupBy: array of ordinals (1 based) or expressions (optional) */
  groupBy?: (number | JsonQLExpr)[]
  /** orderBy: array of { ordinal: (1 based) or expr: expression, direction: "asc"/"desc" (default asc), nulls: "last"/"first" (default is not set) } (optional) */
  orderBy?: ({ ordinal: number, direction?: "asc" | "desc", nulls?: "last" | "first" } | { expr: JsonQLExpr, direction?: "asc" | "desc", nulls?: "last" | "first" })[]

  /** Limit number of rows */
  limit?: number
  /** Optional offset in rows */
  offset?: number
  distinct?: boolean

  /** withs: common table expressions (optional). array of { query:, alias: } */
  withs?: { query: JsonQLQuery, alias: string }[]
}

export interface JsonQLExpr {
  // TODO
  type: string
  [other: string]: any
}

export type JsonQLFrom = JsonQLTableFrom | JsonQLJoinFrom | JsonQLSubqueryFrom | JsonQLSubexprFrom

export interface JsonQLJoinFrom {
  type: "join", 
  left: JsonQLFrom 
  right: JsonQLFrom
  kind: "inner" | "left" | "right" | "full" | "cross"
  /** Expression to join on */
  on?: JsonQLExpr
}

export interface JsonQLTableFrom {
  type: "table"
  table: string
  alias: string
}

/** Subquery aliased */
export interface JsonQLSubqueryFrom {
  type: "subquery"
  query: JsonQLQuery
  alias: string
}

/** Subexpression is a from that is an expression, as in select * from someexpression as somealias */
export interface JsonQLSubexprFrom {
  type: "subexpr"
  expr: JsonQLExpr
  alias: string
}

export interface JsonQLSelect {
  type: "select"
  expr: JsonQLExpr
  alias: string
}

/** Scalar subquery */
export interface JsonQLScalar {
  type: "scalar",
  expr: JsonQLExpr
  where?: JsonQLExpr
  from?: JsonQLFrom

  /** orderBy: array of { ordinal: (1 based) or expr: expression, direction: "asc"/"desc" (default asc), nulls: "last"/"first" (default is not set) } (optional) */
  orderBy?: ({ ordinal: number, direction?: "asc" | "desc", nulls?: "last" | "first" } | { expr: JsonQLExpr, direction?: "asc" | "desc", nulls?: "last" | "first" })[]

  /** Limit number of rows */
  limit?: number

  /** withs: common table expressions (optional). array of { query:, alias: } */
  withs?: { query: JsonQLQuery, alias: string }[]
}
