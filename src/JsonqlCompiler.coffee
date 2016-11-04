_ = require 'lodash'
SqlFragment = require './SqlFragment'
QueryOptimizer = require './QueryOptimizer'

# Compiles jsonql to sql
module.exports = class JsonqlCompiler 
  constructor: (schemaMap, optimizeQueries = false) ->
    @schemaMap = schemaMap
    @nextId = 1

    @optimizeQueries = optimizeQueries

  # Compile a query (or union of queries) made up of selects, from, where, order, limit, skip
  # `aliases` are aliases to tables which have a particular row already selected
  # for example, a subquery can use a value from a parent table (parent_table.some_column) as a scalar
  # expression, so it already has a row selected.
  # ctes are aliases for common table expressions. They are a map of alias to true
  compileQuery: (query, aliases = {}, ctes = {}) ->
    # If union, handle that
    if query.type == "union"
      return SqlFragment.join(_.map(query.queries, (q) =>
        new SqlFragment("(").append(@compileQuery(q, aliases, ctes)).append(")")
        ), " union ")

    # If union all, handle that
    if query.type == "union all"
      return SqlFragment.join(_.map(query.queries, (q) =>
        new SqlFragment("(").append(@compileQuery(q, aliases, ctes)).append(")")
        ), " union all ")

    # Optimize query first
    if @optimizeQueries
      query = new QueryOptimizer().optimizeQuery(query)

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

        ctes[w.alias] = true

      frag.append("with ")
      frag.append(SqlFragment.join(withClauses, ", "))
      frag.append(" ")

    frag.append('select ')

    if query.distinct
      frag.append('distinct ')

    # Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    if query.from
      from = @compileFrom(query.from, aliases, ctes)
    else
      from = null

    # Compile selects
    selects = _.map(query.selects, (s) => @compileSelect(s, aliases, ctes))

    # Handle null select
    if selects.length == 0
      frag.append("null")
    else
      frag.append(SqlFragment.join(selects, ", "))

    # Add from
    if from
      frag.append(" from ")
      frag.append(from)

    # Add where
    if query.where
      where = @compileExpr(query.where, aliases, ctes)
      if not where.isEmpty()
        frag.append(" where ")
        frag.append(where)

    # Add group by
    if query.groupBy
      if query.groupBy.length > 0
        frag.append(" group by ")

      # Check that array
      if not _.isArray(query.groupBy)
        throw new Error("Invalid groupBy")

      frag.append(SqlFragment.join(_.map(query.groupBy, (groupBy) =>
        if isInt(groupBy)
          return new SqlFragment("#{groupBy}")
        return @compileExpr(groupBy, aliases, ctes)
        ), ", "))

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
  # aliases are dict of unmapped alias to table name, or true for whitelisted tables (CTEs or subqueries)
  compileSelect: (select, aliases, ctes = {}) ->
    frag = @compileExpr(select.expr, aliases, ctes)

    # Add over
    if select.over
      frag.append(" over (")
      if select.over.partitionBy
        frag.append("partition by ")
        frag.append(SqlFragment.join(
          _.map(select.over.partitionBy, (pb) => @compileExpr(pb, aliases, ctes)), ", "))
      if select.over.orderBy
        frag.append(@compileOrderBy(select.over.orderBy, aliases))
      frag.append(")")

    frag.append(" as ")

    @validateAlias(select.alias)
    frag.append('"' + select.alias + '"')

    return frag

  # Compiles table or join returning sql and modifying aliases
  # ctes are aliases for common table expressions. They are a map of alias to true
  compileFrom: (from, aliases = {}, ctes = {}) ->
    # TODO check that alias is not repeated in from
    switch from.type 
      when "table"
        # Validate alias
        @validateAlias(from.alias)

        # If from cte, alias to true
        if ctes[from.table]
          aliases[from.alias] = true

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
        left = @compileFrom(from.left, aliases, ctes)
        right = @compileFrom(from.right, aliases, ctes)

        # Make sure aliases don't overlap
        if _.intersection(_.keys(left.aliases), _.keys(right.aliases)).length > 0
          throw new Error("Duplicate aliases")

        _.extend(aliases, left.aliases)
        _.extend(aliases, right.aliases)

        # Compile on
        onSql = @compileExpr(from.on, aliases, ctes)

        if from.kind not in ['inner', 'left', 'right']
          throw new Error("Unsupported join kind #{from.kind}")

        # Combine
        return new SqlFragment("(")
            .append(left)
            .append(" " + from.kind + " join ")
            .append(right)
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
        
        # Record alias as true to allow any field to be queried
        aliases[from.alias] = true

        return new SqlFragment("(").append(subquery)
          .append(') as "')
          .append(@schemaMap.mapTableAlias(from.alias))
          .append('"')

      when "subexpr"
        # Validate alias
        @validateAlias(from.alias)

        # If alias already in use, refuse
        if aliases[from.alias]?
          throw new Error("Alias #{from.alias} in use")

        # Compile expression
        subexpr = @compileExpr(from.expr, aliases, ctes)

        # Record alias as true to allow any field to be queried
        aliases[from.alias] = true

        return subexpr.append(' as "')
          .append(@schemaMap.mapTableAlias(from.alias))
          .append('"')

      else
        throw new Error("Unsupported type #{from.type} in #{JSON.stringify(from)}")

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
            f = new SqlFragment("#{o.ordinal}")
          else
            f = @compileExpr(o.expr, aliases)
          if o.direction
            f.append(" " + o.direction)
          if o.nulls and o.nulls in ['first', 'last']
            f.append(" nulls #{o.nulls}")
          return f
        ), ", "))
    return frag

  # Compiles an expression
  # aliases are dict of unmapped alias to table name, or true whitelisted tables (CTEs and subqueries and subexpressions)
  compileExpr: (expr, aliases, ctes = {}) ->
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
        return @compileOpExpr(expr, aliases, ctes)
      when "field"
        # Check that alias exists
        if not aliases[expr.tableAlias]?
          throw new Error("Alias #{expr.tableAlias} unknown")

        # If is true (that is, from a CTE or subquery), allow all but validate column
        if aliases[expr.tableAlias] == true
          # If using column, put x."y"
          if expr.column
            if not expr.column.match(/^[a-z][a-z0-9_]*$/)
              throw new Error("Invalid column #{expr.column}")
            return new SqlFragment(@schemaMap.mapTableAlias(expr.tableAlias)).append('."').append(expr.column).append('"')
          else # Entire row
            return new SqlFragment(@schemaMap.mapTableAlias(expr.tableAlias))

        return @schemaMap.mapColumn(aliases[expr.tableAlias], expr.column, @schemaMap.mapTableAlias(expr.tableAlias))
      when "scalar"
        return @compileScalar(expr, aliases, ctes)
      when "token"
        if expr.token in ["!bbox!", "!scale_denominator!", "!pixel_width!", "!pixel_height!"]
          return new SqlFragment(expr.token)
        throw new Error("Unsupported token #{expr.token}")
      when "case"
        return @compileCaseExpr(expr, aliases, ctes)
      else
        throw new Error("Unsupported type #{expr.type} in #{JSON.stringify(expr)}")

  # Compiles an op expression
  compileOpExpr: (expr, aliases, ctes = {}) ->
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
      "left"
      "right"
      "substr"
      "lpad"
      "rpad"
      "width_bucket"
      "ntile"
      "coalesce"
      "to_json"
      "to_jsonb"
      "convert_to_decimal"  # Custom function used for safely converting to decimal
      "json_build_array"
      "json_build_object"
      "jsonb_build_array"
      "jsonb_build_object"
      "json_object"
      "json_array_elements"
      "jsonb_array_elements"
      "json_array_elements_text"
      "jsonb_array_elements_text"
      "json_typeof"
      "jsonb_typeof"
      "array_to_string"
      "array_agg"
      "lower"
      "upper"
      "round"
      "ceiling"
      "floor"
      "date_part"
      "json_strip_nulls"
      "jsonb_strip_nulls"
      "cos"
      "sin"
      "nullif"
    ]

    switch expr.op
      when ">", "<", ">=", "<=", "=", "<>", "/", "~", "~*", "like", "&&", "->>", "#>>", "@>", '->', '#>', 'in', '?|', "?&"
        frag = new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(new SqlFragment(" " + expr.op + " "))

        if expr.modifier in ['any', 'all']
          frag.append(expr.modifier).append("(")
            .append(@compileExpr(expr.exprs[1], aliases, ctes))
            .append("))")
        else
          frag.append(@compileExpr(expr.exprs[1], aliases, ctes))
            .append(")")
        return frag
      when "and", "or", "+", "-", "*", "||"
        compiledExprs = _.map(expr.exprs, (e) => @compileExpr(e, aliases, ctes))

        # Remove blanks
        compiledExprs = _.filter(compiledExprs, (e) -> not e.isEmpty())

        if compiledExprs.length == 0
          return new SqlFragment()
        else if compiledExprs.length == 1
          return compiledExprs[0]
        else 
          inner = SqlFragment.join(compiledExprs, " " + expr.op + " ")
          return new SqlFragment("(").append(inner).append(")")
      when "is null", "is not null"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(new SqlFragment(" " + expr.op))
          .append(")")
      when "not"
        return new SqlFragment("(not ")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(")")
      when "between"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(" between ")
          .append(@compileExpr(expr.exprs[1], aliases, ctes))
          .append(" and ")
          .append(@compileExpr(expr.exprs[2], aliases, ctes))
          .append(")")
      when "::text", "::geometry", "::geography", "::uuid", "::integer", "::decimal", "::date", "::timestamp", "::boolean", "::uuid[]", "::text[]", "::json", "::jsonb"
        return new SqlFragment("(")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(expr.op)
          .append(")")
      when "exists"
        return new SqlFragment("exists (")
          .append(@compileQuery(expr.exprs[0], aliases, ctes))
          .append(")")
      when "[]"
        return new SqlFragment("((")
          .append(@compileExpr(expr.exprs[0], aliases, ctes))
          .append(")[")
          .append(@compileExpr(expr.exprs[1], aliases, ctes))
          .append("])")

      else
        # Whitelist known functions and all PostGIS
        if expr.op in functions or expr.op.match(/^ST_[a-zA-z]+$/)
          inner = SqlFragment.join(_.map(expr.exprs, (e) => @compileExpr(e, aliases, ctes)), ", ")

          # Handle special case of count(*)
          if expr.op == "count" and inner.isEmpty()
            inner = "*"

          # Handle orderBy
          if expr.orderBy
            inner = inner.append(@compileOrderBy(expr.orderBy, aliases))
            
          return new SqlFragment(expr.op + "(")
            .append(inner)
            .append(")")

        throw new Error("Unsupported op #{expr.op}")

  # Compile a scalar subquery made up of expr, from, where, order, limit, skip
  compileScalar: (query, aliases, ctes = {}) ->
    frag = new SqlFragment('(select ')

    # Make a copy for use internally
    aliases = _.clone(aliases)

    # Compile from clause, getting sql and aliases. Aliases are dict of unmapped alias to table name
    from = @compileFrom(query.from, aliases, ctes)

    # Compile single select expression
    frag.append(@compileExpr(query.expr, aliases, ctes))

    # Add from
    frag.append(" from ")
    frag.append(from)

    # Add where
    if query.where
      where = @compileExpr(query.where, aliases, ctes)
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

  compileCaseExpr: (expr, aliases, ctes = {}) ->
    frag = new SqlFragment('case ')

    if expr.input?
      frag.append(@compileExpr(expr.input, aliases, ctes))
      frag.append(" ")

    for c in expr.cases
      frag.append("when ")
      frag.append(@compileExpr(c.when, aliases, ctes))
      frag.append(" then ")
      frag.append(@compileExpr(c.then, aliases, ctes))
      frag.append(" ")

    if expr.else?
      frag.append("else ")
      frag.append(@compileExpr(expr.else, aliases, ctes))
      frag.append(" ")

    frag.append("end")

  # Validate alias string. Throws if bad
  validateAlias: (alias) ->
    if not alias.match(/^[_a-zA-Z][a-zA-Z_0-9. ]*$/)
      throw new Error("Invalid alias '#{alias}'")

isInt = (x) ->
  return typeof(x)=='number' and (x%1) == 0
