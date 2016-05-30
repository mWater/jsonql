# JsonQL

SQL represented in an unambiguous way in JSON.

* Safely convertible to SQL on server with no injection attacks
* Substitute columns or tables with secured versions on server

Everything is a json object: { type: type of object, ... }. Expressions can be literals for number, string, null and boolean.

## Types

### literal

Literal value. Has:
  type: "literal"
  value: some value (e.g. 5, "apple", etc.). Can be null for a null.

### query

Top level. Has
 selects: [select]
 from: join or table or subquery or subexpression
 where: boolean expression (optional)
 groupBy: array of ordinals (1 based) or expressions (optional)  
 orderBy: array of { ordinal: (1 based) or expr: expression, direction: "asc"/"desc" (default asc) } (optional)
 limit: integer (optional)
 offset: integer (optional)
 withs: common table expressions (optional). array of { query:, alias: }

### op

Expression. Has op:

`>`, `<`, `<>`, `=`, `>=`, `<=`, 
`+`, `-`, `*`, `/`, `~`, `~*`, 
`like`, `and`, `or`, `not`, `is null`, `is not null`, `between`
`avg`, `min`, `max`, `row_number`, etc.
`exists`

For count(*), use count with no expressions.

Has 
 exprs: [expression]
 modifier: "any", "all" (optional)

### case

Case expression. Has:

input: optional input expression
cases: Array of cases. Each has: when, then
else: optional else expression

### select

Contains an expression and alias
{ type: "select", expr: expression, alias: alias of expression }

Can also contain `over` for window functions. Both partitionBy and orderBy are optional
over: { partitionBy: [ list of expressions ], orderBy: [ list of { expr: expression, direction: "asc"/"desc" } ]}

### scalar 

Scalar subquery. Has:
 expr: expr
 where: boolean expression
 from: join or table
 orderBy: optional array of { ordinal: (1 based) or expr: expression, direction: "asc"/"desc" (default asc) }
 limit: integer (optional)

### field

References a field of an aliased table

{ 
	type: "field"
	tableAlias: alias of table
	column: column of field
}

column can be null/undefined to reference the entire row.

### table

Single table, aliased. table can also refer to a CTE made by withs using its alias.

`{ type: "table", table: tablename, alias: somealias }`

### join

Join of two tables or joins.

{ 
	type: "join", 
	left: table or join, 
	right: table or join, 
	kind: "inner"/"left"/"right", 
	on: expression to join on
}

### subquery

query aliased.

`{ type: "subquery", query: subquery query, alias: somealias }`

### subexpr

Subexpression is a from that is an expression, as in select * from someexpression as somealias

`{ type: "subquery", expr: subquery expression, alias: somealias }`

### token

Special literal token, used for PostGIS, etc.
Currently "!bbox!", "!scale_denominator!", "!pixel_width!", "!pixel_height!"

### union

Takes a series of unions

{
	type: "union"
	queries: array of type query
}

### union all

Takes a series of unions

{
	type: "union all"
	queries: array of type query
}