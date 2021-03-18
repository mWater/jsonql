export { default as SqlFragment } from './SqlFragment'
export { default as JsonqlCompiler } from './JsonqlCompiler'
export { default as SchemaMap } from './SchemaMap'

export interface JsonQL {
  type: string
  [other: string]: any
}

/** Takes a series of unions */
export interface JsonQLUnion {
  type: "union"
  queries: JsonQLQuery[]
}

/** Takes a series of unions */
export interface JsonQLUnionAll {
  type: "union all"
  queries: JsonQLQuery[]
}

/** Simple query or union */
export type JsonQLQuery = JsonQLSelectQuery | JsonQLUnion | JsonQLUnionAll

/** Query with a single select at root */
export interface JsonQLSelectQuery {
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

/** JsonQL expression. Can be null */
export type JsonQLExpr = JsonQLLiteral | JsonQLOp | JsonQLCase | JsonQLScalar | JsonQLField | JsonQLToken | null | number | string | boolean

/** Literal value */
export interface JsonQLLiteral {
  type: "literal"
  value: any
}

/** Field value. References a field of an aliased table */
export interface JsonQLField {
  type: "field"
  tableAlias: string
  column: string
}

/**
 * Expression. Has op:
 *
 *  `>`, `<`, `<>`, `=`, `>=`, `<=`, 
 *  `+`, `-`, `*`, `/`, `~`, `~*`, 
 *  `like`, `and`, `or`, `not`, `is null`, `is not null`, `between`
 *  `avg`, `min`, `max`, `row_number`, etc.
 *  `exists`, `[]`, `array_agg`, etc
 *
 *  For count(*), use count with no expressions.
 *
 *  Has 
 *  exprs: [expression]
 *  modifier: "any", "all", "distinct" (optional)
 *  orderBy: array of { expr: expression, direction: "asc"/"desc" } for ordered functions like array_agg(xyz order by abc desc)
 *
 *  Can also contain `over` for window functions. Both partitionBy and orderBy are optional
 *  over: { partitionBy: [ list of expressions ], orderBy: [ list of { expr: expression, direction: "asc"/"desc", nulls: "last"/"first" (default is not set) } ]}
 */
export interface JsonQLOp {
  type: "op"
  op: string
  exprs: JsonQLExpr[]

  /** For = any etc */
  modifier?: "any" | "all" | "distinct"
  
  /** array of { expr: expression, direction: "asc"/"desc" } for ordered functions like array_agg(xyz order by abc desc) */
  orderBy?: { expr: JsonQLExpr, direction: "asc" | "desc" }[]

  /** For window functions */
  over?: {
    partitionBy?: JsonQLExpr[]
    orderBy?: { expr: JsonQLExpr, direction: "asc" | "desc", nulls?: "last" | "first" }[]
  }
}

/** Case expression */
export interface JsonQLCase {
  type: "case"

  /** optional input expression */
  input?: JsonQLExpr

  cases: { when: JsonQLExpr, then: JsonQLExpr }[]

  /** optional else expression */
  else?: JsonQLExpr
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

/** Special literal token, used for PostGIS, etc.
 * Currently "!bbox!", "!scale_denominator!", "!pixel_width!", "!pixel_height!" */
export interface JsonQLToken {
  type: "token"
  token: string
}
