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
  orderBy?: any // TODO
  groupBy?: any // TODO
  limit?: number
  /** Optional offset in rows */
  offset?: number
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
  kind: "inner" | "left" | "right"
  /** Expression to join on */
  on: JsonQLExpr
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
