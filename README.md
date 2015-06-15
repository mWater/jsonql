# JsonQL

SQL represented in an unambiguous way in JSON.

* Safely convertible to SQL on server with no injection attacks
* Substitute columns or tables with secured versions on server

Everything is a json object: { type: type of object, ... }. Expressions can be literals for number, string, null and boolean.

## Types

### query

Top level. Has
 selects: [select]
 from: join or table
 where: boolean expression (optional)
 groupBy: array of ordinals (1 based) (optional)
 orderBy: array of { ordinal: (1 based) or expr: expression, direction: "asc"/"desc" (default asc) } (optional)
 limit: integer (optional)
 offset: integer (optional)

### op

Expression. Has op: >, <, <>, =, >=, <=, +, -, *, /, ~, ~*, like, and, or, not, is null, is not null
Has exprs: [expression]

### select

Contains an expression and alias
{ type: "select", expr: expression, alias: alias of expression }

### scalar 

Scalar subquery. Has { expr: expr, where: boolean expression, from: join or table, order: expr, limit: integer }

### field

References a field of an aliased table

{ 
	type: "field"
	tableAlias: alias of table
	column: column of field
}

### table

Single table, aliased. { type: "table", table: tablename, alias: somealias }

### join

Join of two tables or joins.

{ 
	type: "join", 
	left: table or join, 
	right: table or join, 
	kind: "inner"/"left"/"right", 
	on: expression to join on
}

### token

Special literal token, used for PostGIS, etc.
Currently "!bbox!" only