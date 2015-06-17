_ = require 'lodash'
SqlFragment = require './SqlFragment'

# Compiles jsonql to sql
module.exports = class JsonqlCompiler 
  constructor: (schemaMap) ->
    @schemaMap = schemaMap
    @nextId = 1

  # Compile a query made up of selects, from, where, order, limit, skip
  # `aliases` are aliases to tables which have a particular row already selected
  # for example, a subquery can use a value from a parent table (parent_table.some_column) as a scalar
  # expression, so it already has a row selected.
  # ctes are aliases for common table expressions. They are a map of alias to list of fields.
  compileQuery: (query, aliases = {}, ctes = {}) ->
    frag = new SqlFragment()

    # Make a copy for use internally
    aliases = _.clone(aliases)
    ctes = _.clone(ctes)

    # Compile withs
    if query.withs and query.withs.length > 0
      withClauses = []
      for w in query.withs
        f = new SqlFragment('"').append(@schemaMap.mapTableAlias(w.alias))
        f.append("\" as (")
        f.append(@compileQuery(w.query, aliases))
        f.append(")")
        withClauses.push(f)

        # Add to cte tables
        if ctes[w.alias]
          throw new Error("CTE alias #{w.alias} in use")

        # Get list of fields of cte
        fields = _.map(w.query.selects, (s) -> s.alias)
        ctes[w.alias] = fields

      frag.append("with ")
      frag.append(SqlFragment.join(withClauses, ", "))
      frag.append(" ")

    frag.append('select ')

    # Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    from = @compileFrom(query.from, aliases, ctes)

    # Compile selects
    selects = _.map(query.selects, (s) => @compileSelect(s, aliases))
    frag.append(SqlFragment.join(selects, ", "))

    # Add from
    frag.append(" from ")
    frag.append(from)

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
  # aliases are dict of unmapped alias to table name, or list of fields for whitelisted tables (CTEs)
  compileSelect: (select, aliases) ->
    frag = @compileExpr(select.expr, aliases)

    # Add over
    if select.over
      frag.append(" over (")
      if select.over.partitionBy
        frag.append("partition by ")
        frag.append(SqlFragment.join(
          _.map(select.over.partitionBy, (pb) => @compileExpr(pb, aliases)), ", "))
      frag.append(")")

    frag.append(" as ")

    @validateAlias(select.alias)
    frag.append('"' + select.alias + '"')

    return frag

  # Compiles table or join returning sql and modifying aliases
  # ctes are aliases for common table expressions. They are a map of alias to list of fields.
  compileFrom: (from, aliases = {}, ctes = {}) ->
    switch from.type 
      when "table"
        # Validate alias
        @validateAlias(from.alias)

        # If alias already in use, refuse
        if aliases[from.alias]?
          throw new Error("Alias #{from.alias} in use")

        # If from cte, alias to list of fields
        if ctes[from.table]
          aliases[from.alias] = ctes[from.table]

          # Reference the CTE by its alias and alias the resulting table
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
      when "subquery"
        # Validate alias
        @validateAlias(from.alias)

        # If alias already in use, refuse
        if aliases[from.alias]?
          throw new Error("Alias #{from.alias} in use")

        # Compile query
        subquery = @compileQuery(from.query, aliases, ctes)

        # Get list of fields of subquery
        fields = _.map(from.query.selects, (s) -> s.alias)
        
        # Record alias as a list of fields
        aliases[from.alias] = fields

        return new SqlFragment("(").append(subquery)
          .append(') as "')
          .append(@schemaMap.mapTableAlias(from.alias))
          .append('"')

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
  # aliases are dict of unmapped alias to table name, or list of fields for whitelisted tables (CTEs)
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
        if not aliases[expr.tableAlias]?
          throw new Error("Alias #{expr.tableAlias} unknown")

        # If a list of fields (from a CTE), check that field is known
        if _.isArray(aliases[expr.tableAlias])
          if expr.column not in aliases[expr.tableAlias]
            throw new Error("Unknown column #{expr.column} of #{expr.tableAlias}")

          return new SqlFragment(@schemaMap.mapTableAlias(expr.tableAlias)).append('.').append(expr.column)

        return @schemaMap.mapColumn(aliases[expr.tableAlias], expr.column, @schemaMap.mapTableAlias(expr.tableAlias))
      when "scalar"
        return @compileScalar(expr, aliases)
      when "token"
        if expr.token in ["!bbox!", "!scale_denominator!", "!pixel_width!", "!pixel_height!"]
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
      "row_number"
    ]

    switch expr.op
      when ">", "<", ">=", "<=", "=", "<>", "+", "-", "*", "/", "~", "~*", "like", "&&"
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
      when "::text", "::geometry", "::geography", "::uuid", "::integer", "::decimal"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases))
          .append(expr.op)
          .append(")")
      else
        # Whitelist known functions and all PostGIS
        if expr.op in functions or expr.op.match(/^ST_[a-zA-z]+$/)
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

    # Compile single select expression
    frag.append(@compileExpr(query.expr, aliases))

    # Add from
    frag.append(" from ")
    frag.append(from)

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
