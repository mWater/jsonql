_ = require 'lodash'
SqlFragment = require './SqlFragment'

# Compiles jsonql to sql
module.exports = class JsonqlCompiler 
  constructor: (schemaMap) ->
    @schemaMap = schemaMap
    @nextId = 1

  # Compile a query made up of selects, from, where, order, limit, skip
  # Aliases are known table aliases to use. Alias that maps to "" is a CTE or subquery that does not need 
  # escaping
  compileQuery: (query, aliases = {}) ->
    frag = new SqlFragment()

    # Make a copy for use internally
    aliases = _.clone(aliases)

    # Compile withs
    if query.withs and query.withs.length > 0
      withClauses = []
      for w in query.withs
        f = new SqlFragment('"').append(@schemaMap.mapTableAlias(w.alias))
        f.append("\" as (")
        f.append(@compileQuery(w.query, aliases))
        f.append(")")
        withClauses.push(f)

        # Add aliases to "" to indicate that CTE aliases are safe
        aliases[w.alias] = ""

      console.log withClauses
      frag.append("with ")
      frag.append(SqlFragment.join(withClauses, ", "))
      frag.append(" ")

    frag.append('select ')

    # Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    from = @compileFrom(query.from, aliases)

    # Compile selects
    selects = _.map(query.selects, (s) => @compileSelect(s, aliases))
    frag.append(SqlFragment.join(selects, ", "))

    # Add from
    frag.append(" from ")
    frag.append(from.sql)

    # Add where
    if query.where
      where = @compileExpr(query.where, aliases)
      if not where.isEmpty()
        frag.append(" where ")
        frag.append(where)

    # Add group by
    if query.groupBy
      # Check that are ints
      if not _.isArray(query.groupBy) or not _.all(query.groupBy, isInt)
        throw new Error("Invalid groupBy")
      if query.groupBy.length > 0
        frag.append(" group by ")
          .append(SqlFragment.join(_.map(query.groupBy, (g) -> new SqlFragment("?", [g])), ", "))

    # Add order by
    if query.orderBy
      frag.append(@compileOrderBy(query.orderBy, aliases))

    # Add limit
    if query.limit?
      # Check that is int
      if not isInt(query.limit)
        throw new Error("Invalid limit")
      frag.append(" limit ")
        .append(new SqlFragment("?", [query.limit]))

    # Add offset
    if query.offset?
      # Check that is int
      if not isInt(query.offset)
        throw new Error("Invalid offset")
      frag.append(" offset ")
        .append(new SqlFragment("?", [query.offset]))

    return frag

  # select is { expr: <expr>, alias: <string> }
  # aliases are dict of unmapped alias to table name
  compileSelect: (select, aliases) ->
    frag = @compileExpr(select.expr, aliases)
    frag.append(" as ")

    @validateAlias(select.alias)
    frag.append('"' + select.alias + '"')

    return frag

  # Compiles table or join returning sql and modifying aliases
  compileFrom: (from, aliases) ->
    switch from.type 
      when "table"
        # Validate alias
        @validateAlias(from.alias)

        # If alias already in use, refuse
        if aliases[from.alias]?
          throw new Error("Alias #{from.alias} in use")

        # If from.table is an existing alias, use it directly
        if aliases[from.table]?
          # Save alias
          aliases[from.alias] = "" # Save as known non-table

          return new SqlFragment(@schemaMap.mapTableAlias(from.table))
            .append(' as "')
            .append(@schemaMap.mapTableAlias(from.alias))
            .append('"')

        # Save alias
        aliases[from.alias] = from.table
        return @schemaMap.mapTable(from.table).append(new SqlFragment(' as "' + @schemaMap.mapTableAlias(from.alias) + '"'))

      when "join"
        # Compile left and right
        left = @compileFrom(from.left, aliases)
        right = @compileFrom(from.right, aliases)

        # Make sure aliases don't overlap
        if _.intersection(_.keys(left.aliases), _.keys(right.aliases)).length > 0
          throw new Error("Duplicate aliases")

        _.extend(aliases, left.aliases)
        _.extend(aliases, right.aliases)

        # Compile on
        onSql = @compileExpr(from.on, aliases)

        if from.kind not in ['inner', 'left', 'right']
          throw new Error("Unsupported join kind #{from.kind}")

        # Combine
        return new SqlFragment("(")
            .append(left.sql)
            .append(" " + from.kind + " join ")
            .append(right.sql)
            .append(" on ")
            .append(onSql)
            .append(")")
      else
        throw new Error("Unsupported type #{from.type}")

  compileOrderBy: (orderBy, aliases) ->
    frag = new SqlFragment()

    if not _.isArray(orderBy)
      throw new Error("Invalid orderBy")

    if not _.all(orderBy, (o) =>
        if not isInt(o.ordinal) and not o.expr
          return false

        return not o.direction? or o.direction in ['asc', 'desc']
      )
      throw new Error("Invalid orderBy")

    if orderBy.length > 0
      frag.append(" order by ")
        .append(SqlFragment.join(_.map(orderBy, (o) => 
          if _.isNumber(o.ordinal)
            f = new SqlFragment("?", [o.ordinal])
          else
            f = @compileExpr(o.expr, aliases)
          if o.direction
            f.append(" " + o.direction)
          return f
        ), ", "))
    return frag

  # Compiles an expression
  compileExpr: (expr, aliases) ->
    if not aliases?
      throw new Error("Missing aliases")

    if not expr?
      return new SqlFragment("null")

    # Literals
    if typeof(expr) in ["number", "string", "boolean"]
      return new SqlFragment("?", [expr])

    switch expr.type
      when "literal"
        return new SqlFragment("?", [expr.value])
      when "op"
        return @compileOpExpr(expr, aliases)
      when "field"
        # Check that alias exists
        if not aliases[expr.tableAlias]
          throw new Error("Alias #{expr.tableAlias} unknown")
        return @schemaMap.mapColumn(aliases[expr.tableAlias], expr.column, @schemaMap.mapTableAlias(expr.tableAlias))
      when "scalar"
        return @compileScalar(expr, aliases)
      when "token"
        if expr.token in ['!bbox!']
          return new SqlFragment(expr.token)
        throw new Error("Unsupported token #{expr.token}")
      else
        throw new Error("Unsupported type #{expr.type}")

  # Compiles an op expression
  compileOpExpr: (expr, aliases) ->
    functions = [
      "avg"
      "min"
      "max"
      "sum"
      "count"
      "stdev"
      "stdevp"
      "var"
      "varp"
      "ST_Transform"
    ]

    switch expr.op
      when ">", "<", ">=", "<=", "=", "<>", "+", "-", "*", "/", "~", "~*", "like"
        frag = new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases))
          .append(new SqlFragment(" " + expr.op + " "))

        if expr.modifier in ['any', 'all']
          frag.append(expr.modifier).append("(")
            .append(@compileExpr(expr.exprs[1], aliases))
            .append("))")
        else
          frag.append(@compileExpr(expr.exprs[1], aliases))
            .append(")")
        return frag
      when "and", "or"
        if expr.exprs.length == 0
          return new SqlFragment()
        else if expr.exprs.length == 1
          return @compileExpr(expr.exprs[0], aliases)
        else 
          inner = SqlFragment.join(_.map(expr.exprs, (e) => @compileExpr(e, aliases)), " " + expr.op + " ")
          return new SqlFragment("(").append(inner).append(")")
      when "is null", "is not null"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases))
          .append(new SqlFragment(" " + expr.op))
          .append(")")
      when "not"
        return new SqlFragment("(not ")
          .append(@compileExpr(expr.exprs[0], aliases))
          .append(")")
      when "::text"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases))
          .append("::text)")
      else
        if expr.op in functions
          inner = SqlFragment.join(_.map(expr.exprs, (e) => @compileExpr(e, aliases)), ", ")
          return new SqlFragment(expr.op + "(")
            .append(inner)
            .append(")")

        throw new Error("Unsupported op #{expr.op}")

  # Compile a scalar subquery made up of expr, from, where, order, limit, skip
  compileScalar: (query, aliases) ->
    frag = new SqlFragment('(select ')

    # Make a copy for use internally
    aliases = _.clone(aliases)

    # Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    from = @compileFrom(query.from, aliases)

    # Check that no overlap with existing aliases
    if _.intersection(_.keys(from.aliases), _.keys(aliases)).length > 0
      throw new Error("Re-used alias in scalar subquery")

    # Combine aliases
    aliases = _.extend({}, aliases, from.aliases)

    # Compile single select expression
    frag.append(@compileExpr(query.expr, aliases))

    # Add from
    frag.append(" from ")
    frag.append(from.sql)

    # Add where
    if query.where
      where = @compileExpr(query.where, aliases)
      if not where.isEmpty()
        frag.append(" where ")
        frag.append(where)

    # Add order by
    if query.orderBy
      frag.append(@compileOrderBy(query.orderBy, aliases))

    # Add limit
    if query.limit?
      # Check that is int
      if not isInt(query.limit)
        throw new Error("Invalid limit")
      frag.append(" limit ")
        .append(new SqlFragment("?", [query.limit]))

    # Add offset
    if query.offset?
      # Check that is int
      if not isInt(query.offset)
        throw new Error("Invalid offset")
      frag.append(" offset ")
        .append(new SqlFragment("?", [query.offset]))

    frag.append(")")
    return frag

  # Validate alias string. Throws if bad
  validateAlias: (alias) ->
    if not alias.match(/^[a-zA-Z][a-zA-Z_0-9]*$/)
      throw new Error("Invalid alias #{alias}")

isInt = (x) ->
  return typeof(x)=='number' and (x%1) == 0
