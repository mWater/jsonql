import _ from "lodash"
import SqlFragment from "./SqlFragment"
import QueryOptimizer from "./QueryOptimizer"

// Compiles jsonql to sql
export default class JsonqlCompiler {
  constructor(schemaMap: any, optimizeQueries = false) {
    this.schemaMap = schemaMap
    this.nextId = 1

    this.optimizeQueries = optimizeQueries
  }

  // Compile a query (or union of queries) made up of selects, from, where, order, limit, skip
  // `aliases` are aliases to tables which have a particular row already selected
  // for example, a subquery can use a value from a parent table (parent_table.some_column) as a scalar
  // expression, so it already has a row selected.
  // ctes are aliases for common table expressions. They are a map of alias to true
  compileQuery(query: any, aliases = {}, ctes = {}) {
    // If union, handle that
    let from
    if (query.type === "union") {
      return SqlFragment.join(
        _.map(query.queries, (q: any) => {
          return new SqlFragment("(").append(this.compileQuery(q, aliases, ctes)).append(")")
        }),
        " union "
      )
    }

    // If union all, handle that
    if (query.type === "union all") {
      return SqlFragment.join(
        _.map(query.queries, (q: any) => {
          return new SqlFragment("(").append(this.compileQuery(q, aliases, ctes)).append(")")
        }),
        " union all "
      )
    }

    // Optimize query first
    if (this.optimizeQueries) {
      query = new QueryOptimizer().optimizeQuery(query)
    }

    const frag = new SqlFragment()

    // Make a copy for use internally
    aliases = _.clone(aliases)
    ctes = _.clone(ctes)

    // Compile withs
    if (query.withs && query.withs.length > 0) {
      const withClauses = []
      for (let w of query.withs) {
        const f = new SqlFragment('"').append(this.schemaMap.mapTableAlias(w.alias))
        f.append('" as (')
        f.append(this.compileQuery(w.query, aliases))
        f.append(")")
        withClauses.push(f)

        // Add to cte tables
        if (ctes[w.alias]) {
          throw new Error(`CTE alias ${w.alias} in use`)
        }

        ctes[w.alias] = true
      }

      frag.append("with ")
      frag.append(SqlFragment.join(withClauses, ", "))
      frag.append(" ")
    }

    frag.append("select ")

    if (query.distinct) {
      frag.append("distinct ")
    }

    // Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    if (query.from) {
      from = this.compileFrom(query.from, aliases, ctes)
    } else {
      from = null
    }

    // Compile selects
    const selects = _.map(query.selects, (s: any) => this.compileSelect(s, aliases, ctes))

    // Handle null select
    if (selects.length === 0) {
      frag.append("null")
    } else {
      frag.append(SqlFragment.join(selects, ", "))
    }

    // Add from
    if (from) {
      frag.append(" from ")
      frag.append(from)
    }

    // Add where
    if (query.where != null) {
      const where = this.compileExpr(query.where, aliases, ctes)
      if (!where.isEmpty()) {
        frag.append(" where ")
        frag.append(where)
      }
    }

    // Add group by
    if (query.groupBy) {
      if (query.groupBy.length > 0) {
        frag.append(" group by ")
      }

      // Check that array
      if (!_.isArray(query.groupBy)) {
        throw new Error("Invalid groupBy")
      }

      frag.append(
        SqlFragment.join(
          _.map(query.groupBy, (groupBy: any) => {
            if (isInt(groupBy)) {
              return new SqlFragment(`${groupBy}`)
            }
            return this.compileExpr(groupBy, aliases, ctes)
          }),
          ", "
        )
      )
    }

    // Add order by
    if (query.orderBy) {
      frag.append(this.compileOrderBy(query.orderBy, aliases))
    }

    // Add limit
    if (query.limit != null) {
      // Check that is int
      if (!isInt(query.limit)) {
        throw new Error("Invalid limit")
      }
      frag.append(" limit ").append(new SqlFragment("?", [query.limit]))
    }

    // Add offset
    if (query.offset != null) {
      // Check that is int
      if (!isInt(query.offset)) {
        throw new Error("Invalid offset")
      }
      frag.append(" offset ").append(new SqlFragment("?", [query.offset]))
    }

    return frag
  }

  // select is { expr: <expr>, alias: <string> }
  // aliases are dict of unmapped alias to table name, or true for whitelisted tables (CTEs or subqueries)
  compileSelect(select: any, aliases: any, ctes = {}) {
    // Add legacy over to expr
    let { expr } = select
    if (select.over) {
      expr = _.extend({}, expr, { over: select.over })
    }

    const frag = this.compileExpr(expr, aliases, ctes)

    frag.append(" as ")

    this.validateAlias(select.alias)
    frag.append('"' + select.alias + '"')

    return frag
  }

  // Compiles table or join returning sql and modifying aliases
  // ctes are aliases for common table expressions. They are a map of alias to true
  compileFrom(from: any, aliases = {}, ctes = {}) {
    // TODO check that alias is not repeated in from
    switch (from.type) {
      case "table":
        // Validate alias
        this.validateAlias(from.alias)

        // If from cte, alias to true
        if (ctes[from.table]) {
          aliases[from.alias] = true

          // Reference the CTE by its alias and alias the resulting table
          return new SqlFragment(this.schemaMap.mapTableAlias(from.table))
            .append(' as "')
            .append(this.schemaMap.mapTableAlias(from.alias))
            .append('"')
        }

        // Save alias
        aliases[from.alias] = from.table
        return this.schemaMap
          .mapTable(from.table)
          .append(new SqlFragment(' as "' + this.schemaMap.mapTableAlias(from.alias) + '"'))

      case "join":
        // Compile left and right
        var left = this.compileFrom(from.left, aliases, ctes)
        var right = this.compileFrom(from.right, aliases, ctes)

        // Make sure aliases don't overlap
        if (_.intersection(_.keys(left.aliases), _.keys(right.aliases)).length > 0) {
          throw new Error("Duplicate aliases")
        }

        _.extend(aliases, left.aliases)
        _.extend(aliases, right.aliases)

        // Ensure that on is present for non-cross
        if (from.kind !== "cross" && from.on == null) {
          throw new Error("Missing on clause for non-cross join")
        }

        // Compile on
        var onSql = from.on ? this.compileExpr(from.on, aliases, ctes) : undefined

        if (!["inner", "left", "right", "full", "cross"].includes(from.kind)) {
          throw new Error(`Unsupported join kind ${from.kind}`)
        }

        // Combine
        var frag = new SqlFragment("(")
          .append(left)
          .append(" " + from.kind + " join ")
          .append(right)
        if (onSql) {
          frag.append(" on ").append(onSql)
        }

        frag.append(")")

        return frag

      case "subquery":
        // Validate alias
        this.validateAlias(from.alias)

        // If alias already in use, refuse
        if (aliases[from.alias] != null) {
          throw new Error(`Alias ${from.alias} in use`)
        }

        // Compile query
        var subquery = this.compileQuery(from.query, aliases, ctes)

        // Get list of fields of subquery
        var fields = _.map(from.query.selects, (s: any) => s.alias)

        // Record alias as true to allow any field to be queried
        aliases[from.alias] = true

        return new SqlFragment("(")
          .append(subquery)
          .append(') as "')
          .append(this.schemaMap.mapTableAlias(from.alias))
          .append('"')

      case "subexpr":
        // Validate alias
        this.validateAlias(from.alias)

        // If alias already in use, refuse
        if (aliases[from.alias] != null) {
          throw new Error(`Alias ${from.alias} in use`)
        }

        // Compile expression
        var subexpr = this.compileExpr(from.expr, aliases, ctes)

        // Record alias as true to allow any field to be queried
        aliases[from.alias] = true

        return subexpr.append(' as "').append(this.schemaMap.mapTableAlias(from.alias)).append('"')

      default:
        throw new Error(`Unsupported type ${from.type} in ${JSON.stringify(from)}`)
    }
  }

  compileOrderBy(orderBy: any, aliases: any) {
    const frag = new SqlFragment()

    if (!_.isArray(orderBy)) {
      throw new Error("Invalid orderBy")
    }

    if (
      !_.all(orderBy, (o: any) => {
        if (!isInt(o.ordinal) && !o.expr) {
          return false
        }

        return o.direction == null || ["asc", "desc"].includes(o.direction)
      })
    ) {
      throw new Error("Invalid orderBy")
    }

    if (orderBy.length > 0) {
      frag.append(" order by ").append(
        SqlFragment.join(
          _.map(orderBy, (o: any) => {
            let f
            if (_.isNumber(o.ordinal)) {
              f = new SqlFragment(`${o.ordinal}`)
            } else {
              f = this.compileExpr(o.expr, aliases)
            }
            if (o.direction) {
              f.append(" " + o.direction)
            }
            if (o.nulls && ["first", "last"].includes(o.nulls)) {
              f.append(` nulls ${o.nulls}`)
            }
            return f
          }),
          ", "
        )
      )
    }
    return frag
  }

  // Compiles an expression
  // aliases are dict of unmapped alias to table name, or true whitelisted tables (CTEs and subqueries and subexpressions)
  compileExpr(expr: any, aliases: any, ctes = {}) {
    if (aliases == null) {
      throw new Error("Missing aliases")
    }

    if (expr == null) {
      return new SqlFragment("null")
    }

    // Literals
    if (["number", "string", "boolean"].includes(typeof expr)) {
      return new SqlFragment("?", [expr])
    }

    switch (expr.type) {
      case "literal":
        return new SqlFragment("?", [expr.value])
      case "op":
        return this.compileOpExpr(expr, aliases, ctes)
      case "field":
        // Check that alias exists
        if (aliases[expr.tableAlias] == null) {
          throw new Error(`Alias ${expr.tableAlias} unknown`)
        }

        // If is true (that is, from a CTE or subquery), allow all but validate column
        if (aliases[expr.tableAlias] === true) {
          // If using column, put x."y"
          if (expr.column) {
            if (!expr.column.match(/^[a-z][a-z0-9_]*$/)) {
              throw new Error(`Invalid column ${expr.column}`)
            }
            return new SqlFragment(this.schemaMap.mapTableAlias(expr.tableAlias))
              .append('."')
              .append(expr.column)
              .append('"')
          } else {
            // Entire row
            return new SqlFragment(this.schemaMap.mapTableAlias(expr.tableAlias))
          }
        }

        return this.schemaMap.mapColumn(
          aliases[expr.tableAlias],
          expr.column,
          this.schemaMap.mapTableAlias(expr.tableAlias)
        )
      case "scalar":
        return this.compileScalar(expr, aliases, ctes)
      case "token":
        if (["!bbox!", "!scale_denominator!", "!pixel_width!", "!pixel_height!"].includes(expr.token)) {
          return new SqlFragment(expr.token)
        }
        throw new Error(`Unsupported token ${expr.token}`)
      case "case":
        return this.compileCaseExpr(expr, aliases, ctes)
      default:
        throw new Error(`Unsupported type ${expr.type} in ${JSON.stringify(expr)}`)
    }
  }

  // Compiles an op expression
  compileOpExpr(expr: any, aliases: any, ctes = {}) {
    let inner
    const functions = [
      "avg",
      "min",
      "max",
      "sum",
      "count",
      "stdev",
      "stdevp",
      "var",
      "varp",
      "row_number",
      "left",
      "right",
      "substr",
      "lpad",
      "rpad",
      "width_bucket",
      "ntile",
      "coalesce",
      "to_json",
      "to_jsonb",
      "to_char",
      "convert_to_decimal", // Custom function used for safely converting to decimal
      "json_build_array",
      "json_build_object",
      "jsonb_build_array",
      "jsonb_build_object",
      "json_array_length",
      "jsonb_array_length",
      "json_object",
      "json_array_elements",
      "jsonb_array_elements",
      "json_array_elements_text",
      "jsonb_array_elements_text",
      "json_typeof",
      "jsonb_typeof",
      "array_to_string",
      "array_agg",
      "lower",
      "upper",
      "round",
      "ceiling",
      "floor",
      "date_part",
      "json_strip_nulls",
      "jsonb_strip_nulls",
      "cos",
      "sin",
      "nullif",
      "log",
      "ln",
      "unnest",
      "now",
      "split_part",
      "chr",
      "least",
      "greatest",
      "bool_or",
      "bool_and"
    ]

    switch (expr.op) {
      case ">":
      case "<":
      case ">=":
      case "<=":
      case "=":
      case "<>":
      case "/":
      case "~":
      case "~*":
      case "like":
      case "ilike":
      case "&&":
      case "->>":
      case "#>>":
      case "@>":
      case "<@":
      case "->":
      case "#>":
      case "in":
      case "?|":
      case "?&":
        var frag = new SqlFragment("(")
          .append(this.compileExpr(expr.exprs[0], aliases, ctes))
          .append(new SqlFragment(" " + expr.op + " "))

        if (["any", "all"].includes(expr.modifier)) {
          frag.append(expr.modifier).append("(").append(this.compileExpr(expr.exprs[1], aliases, ctes)).append("))")
        } else {
          frag.append(this.compileExpr(expr.exprs[1], aliases, ctes)).append(")")
        }
        return frag
      case "and":
      case "or":
      case "+":
      case "-":
      case "*":
      case "||":
        var compiledExprs = _.map(expr.exprs, (e: any) => this.compileExpr(e, aliases, ctes))

        // Remove blanks
        compiledExprs = _.filter(compiledExprs, (e: any) => !e.isEmpty())

        if (compiledExprs.length === 0) {
          return new SqlFragment()
        } else if (compiledExprs.length === 1) {
          return compiledExprs[0]
        } else {
          inner = SqlFragment.join(compiledExprs, " " + expr.op + " ")
          return new SqlFragment("(").append(inner).append(")")
        }
      case "is null":
      case "is not null":
        return new SqlFragment("(")
          .append(this.compileExpr(expr.exprs[0], aliases, ctes))
          .append(new SqlFragment(" " + expr.op))
          .append(")")
      case "not":
        return new SqlFragment("(not ").append(this.compileExpr(expr.exprs[0], aliases, ctes)).append(")")
      case "between":
        return new SqlFragment("(")
          .append(this.compileExpr(expr.exprs[0], aliases, ctes))
          .append(" between ")
          .append(this.compileExpr(expr.exprs[1], aliases, ctes))
          .append(" and ")
          .append(this.compileExpr(expr.exprs[2], aliases, ctes))
          .append(")")
      case "::text":
      case "::geometry":
      case "::geography":
      case "::uuid":
      case "::integer":
      case "::decimal":
      case "::date":
      case "::timestamp":
      case "::boolean":
      case "::uuid[]":
      case "::text[]":
      case "::json":
      case "::jsonb":
      case "::jsonb[]":
      case "::spheroid":
      case "::numeric":
      case "::integer[]":
        return new SqlFragment("(").append(this.compileExpr(expr.exprs[0], aliases, ctes)).append(expr.op).append(")")
      case "exists":
        return new SqlFragment("exists (").append(this.compileQuery(expr.exprs[0], aliases, ctes)).append(")")
      case "[]":
        return new SqlFragment("((")
          .append(this.compileExpr(expr.exprs[0], aliases, ctes))
          .append(")[")
          .append(this.compileExpr(expr.exprs[1], aliases, ctes))
          .append("])")
      case "interval":
        return new SqlFragment("(interval ").append(this.compileExpr(expr.exprs[0], aliases, ctes)).append(")")
      case "at time zone":
        return new SqlFragment("(")
          .append(this.compileExpr(expr.exprs[0], aliases, ctes))
          .append(" at time zone ")
          .append(this.compileExpr(expr.exprs[1], aliases, ctes))
          .append(")")
      default:
        // Whitelist known functions and all PostGIS and CartoDb and mwater
        if (
          functions.includes(expr.op) ||
          expr.op.match(/^ST_[a-zA-z]+$/) ||
          expr.op.match(/^CDB_[a-zA-z]+$/) ||
          expr.op.match(/^mwater_[a-zA-z_]+$/)
        ) {
          inner = SqlFragment.join(
            _.map(expr.exprs, (e: any) => this.compileExpr(e, aliases, ctes)),
            ", "
          )

          // Handle special case of count(*)
          if (expr.op === "count" && inner.isEmpty()) {
            inner = "*"
          }

          // Handle orderBy
          if (expr.orderBy) {
            inner = inner.append(this.compileOrderBy(expr.orderBy, aliases))
          }

          if (expr.modifier === "distinct") {
            inner = new SqlFragment("distinct ").append(inner)
          }

          frag = new SqlFragment(expr.op + "(").append(inner).append(")")

          if (expr.over) {
            frag = new SqlFragment("(").append(frag)
            frag.append(" over (")
            if (expr.over.partitionBy) {
              frag.append("partition by ")
              frag.append(
                SqlFragment.join(
                  _.map(expr.over.partitionBy, (pb: any) => this.compileExpr(pb, aliases, ctes)),
                  ", "
                )
              )
            }
            if (expr.over.orderBy) {
              frag.append(this.compileOrderBy(expr.over.orderBy, aliases))
            }
            frag.append("))")
          }

          return frag
        }

        throw new Error(`Unsupported op ${expr.op}`)
    }
  }

  // Compile a scalar subquery made up of expr, from, where, order, limit, skip
  compileScalar(query: any, aliases: any, ctes = {}) {
    let from
    const frag = new SqlFragment("(")

    // Make a copy for use internally
    aliases = _.clone(aliases)
    ctes = _.clone(ctes)

    // Compile withs
    if (query.withs && query.withs.length > 0) {
      const withClauses = []
      for (let w of query.withs) {
        const f = new SqlFragment('"').append(this.schemaMap.mapTableAlias(w.alias))
        f.append('" as (')
        f.append(this.compileQuery(w.query, aliases))
        f.append(")")
        withClauses.push(f)

        // Add to cte tables
        if (ctes[w.alias]) {
          throw new Error(`CTE alias ${w.alias} in use`)
        }

        ctes[w.alias] = true
      }

      frag.append("with ")
      frag.append(SqlFragment.join(withClauses, ", "))
      frag.append(" ")
    }

    frag.append("select ")

    // Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    if (query.from) {
      from = this.compileFrom(query.from, aliases, ctes)
    } else {
      from = null
    }

    // Compile single select expression
    frag.append(this.compileExpr(query.expr, aliases, ctes))

    // Add from
    if (from) {
      frag.append(" from ")
      frag.append(from)
    }

    // Add where
    if (query.where != null) {
      const where = this.compileExpr(query.where, aliases, ctes)
      if (!where.isEmpty()) {
        frag.append(" where ")
        frag.append(where)
      }
    }

    // Add order by
    if (query.orderBy) {
      frag.append(this.compileOrderBy(query.orderBy, aliases))
    }

    // Add limit
    if (query.limit != null) {
      // Check that is int
      if (!isInt(query.limit)) {
        throw new Error("Invalid limit")
      }
      frag.append(" limit ").append(new SqlFragment("?", [query.limit]))
    }

    // Add offset
    if (query.offset != null) {
      // Check that is int
      if (!isInt(query.offset)) {
        throw new Error("Invalid offset")
      }
      frag.append(" offset ").append(new SqlFragment("?", [query.offset]))
    }

    frag.append(")")
    return frag
  }

  compileCaseExpr(expr: any, aliases: any, ctes = {}) {
    const frag = new SqlFragment("case ")

    if (expr.input != null) {
      frag.append(this.compileExpr(expr.input, aliases, ctes))
      frag.append(" ")
    }

    for (let c of expr.cases) {
      frag.append("when ")
      frag.append(this.compileExpr(c.when, aliases, ctes))
      frag.append(" then ")
      frag.append(this.compileExpr(c.then, aliases, ctes))
      frag.append(" ")
    }

    if (expr.else != null) {
      frag.append("else ")
      frag.append(this.compileExpr(expr.else, aliases, ctes))
      frag.append(" ")
    }

    return frag.append("end")
  }

  // Validate alias string. Throws if bad
  validateAlias(alias: any) {
    if (!alias.match(/^[_a-zA-Z][a-zA-Z_0-9. :]*$/)) {
      throw new Error(`Invalid alias '${alias}'`)
    }
  }
}

function isInt(x: any) {
  return typeof x === "number" && x % 1 === 0
}
