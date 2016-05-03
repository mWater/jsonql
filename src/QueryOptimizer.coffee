_ = require 'lodash'
###

Scalar subqueries can be very slow in Postgresql as they are not re-written but instead loop over and over.

This attempts to re-write them as left outer joins, which is a complex tranformation.

There are three cases: 

1) non-aggregated subquery. These are just a left outer join in an inner query

2) aggregated subquery. These are sum(some value), etc as the thing being selected in the scalar

3) limit 1 subquery. These are taking the *latest* of some value, for example, and have an order by and limit 1

When the scalar is in the where clause, the where clause is processed first, splitting into "ands" and putting 
as much as possible in the inner query for speed.

We need to do trickery with row_number to give the wrapping queries something to group on or partition by.

We re-write wheres first, followed by selects, followed by order bys.

See the tests for examples of all three re-writings. The speed difference is 1000x plus depending on the # of rows.

###

module.exports = class QueryOptimizer
  debugQuery: (query) ->
    SchemaMap = require './SchemaMap'
    JsonqlCompiler = require './JsonqlCompiler'

    try
      sql = new JsonqlCompiler(new SchemaMap(), false).compileQuery(query)
      console.log "===== SQL ======"
      console.log sql.toInline()
      console.log "================"
    catch ex
      console.trace("Failure?")
      console.log "FAILURE: " + ex.message
      console.log JSON.stringify(query, null, 2)

  # Run rewriteScalar query repeatedly until no more changes
  optimizeQuery: (query, debug = true) ->
    if debug
      console.log "================== BEFORE OPT ================"
      @debugQuery(query)

    for i in [0...20]
      optQuery = @rewriteScalar(query)

      if optQuery == query
        return optQuery

      if debug
        console.log "================== OPT #{i} ================"
        @debugQuery(optQuery)

      query = optQuery

    throw new Error("Unable to optimize query (infinite loop): #{JSON.stringify(query)}")

  rewriteScalar: (query) ->
    # Find scalar to optimize
    scalar = @findScalar(query)

    if not scalar
      return query

    # Get table aliases in from
    fromAliases = @extractFromAliases(query.from)

    # Get all fields
    fields = @extractFields(query)

    # Filter fields to ones that reference from clause
    fields = _.filter(fields, (f) -> f.tableAlias in fromAliases)

    # Unique fields
    fields = _.uniq(fields, (f) -> "#{f.tableAlias}::#{f.column}")

    # Split where into ands
    wheres = []
    if query.where and query.where.type == "op" and query.where.op == "and"
      wheres = query.where.exprs
    else if query.where
      # Single expression
      wheres = [query.where]

    # Split inner where (not containing the scalar) and outer wheres (containing the scalar)
    innerWhere = { type: "op", op: "and", exprs: _.filter(wheres, (where) =>
      @findScalar(where) != scalar
      ) }

    outerWhere = { type: "op", op: "and", exprs: _.filter(wheres, (where) =>
      @findScalar(where) == scalar
      ) }

    # Null if empty
    if innerWhere.exprs.length == 0
      innerWhere = null

    if outerWhere.exprs.length == 0
      outerWhere = null

    # Remaps over clause in select
    remapOver = (over, alias) =>
      if not over
        return over

      return _.omit({
        partitionBy: if over.partitionBy then _.map(over.partitionBy, (pb) => @remapFields(pb, fields, scalar, alias))
        orderBy: if over.orderBy then _.map(over.orderBy, (ob) => _.extend({}, ob, { expr: @remapFields(ob.expr, fields, scalar, alias) }))
        }, _.isUndefined)

    # Remaps selects for outer query, mapping fields in expr and over clauses
    remapSelects = (selects, alias) =>
      # Re-write query selects to use new opt0 query
      return _.map selects, (select) =>
        # Get rid of undefined values
        _.omit({
          type: "select"
          expr: @remapFields(select.expr, fields, scalar, alias)
          over: remapOver(select.over, alias)
          alias: select.alias
        }, _.isUndefined)
      
    # If simple non-aggregate
    if not @isAggr(scalar.expr) and not scalar.limit
      # Create new selects for opt0 query with all fields + scalar expression
      opt0Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt0Selects.push({ type: "select", expr: scalar.expr, alias: "expr" })

      # Create new opt0 from clause with left outer join to scalar
      opt0From = { type: "join", kind: "left", left: query.from, right: scalar.from, on: scalar.where }

      # Create opt0 query opt0
      opt0Query = {
        type: "query"
        selects: opt0Selects
        from: opt0From
        where: innerWhere
      }

      # Optimize inner query (TODO give each unique name?)
      opt0Query = @optimizeQuery(opt0Query, false)

      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt0 query
        selects: remapSelects(query.selects, "opt0")
        from: {
          type: "subquery"
          query: opt0Query
          alias: "opt0"
        }
        where: @remapFields(outerWhere, fields, scalar, "opt0")
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt0") })
        )
      })
      return outerQuery

    else if not scalar.limit
      # Create new selects for opt0 query with all fields + row number
      opt0Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt0Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" })

      # Create opt0 query opt0
      opt0Query = {
        type: "query"
        selects: opt0Selects
        from: query.from
        where: innerWhere
      }

      # Optimize inner query (TODO give each unique name?)
      opt0Query = @optimizeQuery(opt0Query, false)

      # Create new selects for opt1 query with row number + all fields + scalar expression
      opt1Selects = [{ type: "select", expr: { type: "field", tableAlias: "opt0", column: "rn" }, alias: "rn" }]
      opt1Selects = opt1Selects.concat(_.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      ))
      opt1Selects.push({ type: "select", expr: @remapFields(scalar.expr, fields, null, "opt0"), alias: "expr" })

      # Create new opt1 from clause with left outer join to scalar
      opt1From = { type: "join", kind: "left", left: { type: "subquery", query: opt0Query, alias: "opt0" }, right: scalar.from, on: @remapFields(scalar.where, fields, scalar, "opt0") }

      opt1Query = {
        type: "query"
        selects: opt1Selects
        from: opt1From
        groupBy: _.range(1, fields.length + 2)
      }

      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt1 query
        selects: remapSelects(query.selects, "opt1")
        from: {
          type: "subquery"
          query: opt1Query
          alias: "opt1"
        }
        where: @remapFields(outerWhere, fields, scalar, "opt1")
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt1") })
        )
      })

      return outerQuery

    # Limit scalar
    else 
      # Create new selects for opt0 query with all fields + row number
      opt0Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt0Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" })

      # Create opt0 query opt0
      opt0Query = {
        type: "query"
        selects: opt0Selects
        from: query.from
        where: innerWhere
      }

      # Optimize inner query (TODO give each unique name?)
      opt0Query = @optimizeQuery(opt0Query, false)

      # Create new selects for opt1 query with all fields + scalar expression + ordered row number over inner row number
      opt1Selects = _.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: "opt0", column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt1Selects.push({ type: "select", expr: @remapFields(scalar.expr, fields, null, "opt0"), alias: "expr" })
      opt1Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {
        partitionBy: [{ type: "field", tableAlias: "opt0", column: "rn" }]
        orderBy: scalar.orderBy
        }, alias: "rn" })

      # Create new opt1 from clause with left outer join to scalar
      opt1From = { type: "join", kind: "left", left: { type: "subquery", query: opt0Query, alias: "opt0" }, right: scalar.from, on: @remapFields(scalar.where, fields, scalar, "opt0") }

      opt1Query = {
        type: "query"
        selects: opt1Selects
        from: opt1From
      }

      opt2Selects = _.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: "opt1", column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt2Selects.push({ type: "select", expr: { type: "field", tableAlias: "opt1", column: "expr" }, alias: "expr" })

      opt2Query = {
        type: "query"
        selects: opt2Selects
        from: {
          type: "subquery"
          query: opt1Query
          alias: "opt1"
        }
        where: {
          type: "op"
          op: "="
          exprs: [
            { type: "field", tableAlias: "opt1", column: "rn" }
            { type: "literal", value: 1 }
          ]
        }
      }

      # Wrap in final query
      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt1 query
        selects: remapSelects(query.selects, "opt2")
        from: {
          type: "subquery"
          query: opt2Query
          alias: "opt2"
        }
        where: @remapFields(outerWhere, fields, scalar, "opt2")
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt2") })
        )
      })

      return outerQuery

  # Find a scalar in where, selects or order by or expression
  findScalar: (frag) ->
    if not frag or not frag.type
      return null

    switch frag.type
      when "query"
        # Find in where clause
        scalar = @findScalar(frag.where)
        if scalar
          return scalar
        
        # Find in selects
        for select in frag.selects        
          scalar = @findScalar(select.expr)
          if scalar
            return scalar

        # Find in order by
        if frag.orderBy
          for orderBy in frag.orderBy
            scalar = @findScalar(orderBy.expr)
            if scalar
              return scalar

      when "scalar"
        return frag

      when "op"
        for expr in frag.exprs
          scalar = @findScalar(expr)
          if scalar
            return scalar

    return null

  extractFromAliases: (from) ->
    switch from.type
      when "table", "subquery", "subexpr"
        return [from.alias]
      when "join"
        return @extractFromAliases(from.left).concat(@extractFromAliases(from.right))

    throw new Error("Unknown from type #{from.type}")

  # Extract all jsonql field expressions from a jsonql fragment
  extractFields: (frag) =>
    if not frag or not frag.type
      return []

    switch frag.type
      when "query"
        return _.flatten(_.map(frag.selects, (select) => @extractFields(select.expr))).concat(@extractFields(frag.where)).concat(_.flatten(_.map(frag.orderBy, (orderBy) => @extractFields(orderBy.expr))))
      when "field"
        return [frag]
      when "op"
        return _.flatten(_.map(frag.exprs, @extractFields))
      when "case"
        return @extractFields(frag.input).concat(_.flatten(_.map(frag.cases, (cs) => @extractFields(cs.when).concat(@extractFields(cs.then))))).concat(@extractFields(frag.else))
      when "scalar"
        return @extractFields(frag.frag).concat(@extractFields(frag.where)).concat(_.map(frag.orderBy, (ob) => @extractFields(ob.frag)))
      when "literal"
        return []
      when "token"
        return []
      else
        throw new Error("Unsupported extractFields with type #{frag.type}")

  # Determine if expression is aggregate
  isAggr: (expr) =>
    if not expr or not expr.type
      return false

    switch expr.type
      when "field"
        return false
      when "op"
        return expr.op in ['sum', 'min', 'max', 'avg', 'count', 'stdev', 'stdevp', 'var', 'varp']
      when "case"
        return _.any(expr.cases, (cs) => @isAggr(cs.then))
      when "scalar"
        return false
      when "literal"
        return false
      when "token"
        return false
      else
        throw new Error("Unsupported isAggr with type #{expr.type}")

  # Remap fields a.b1 to format <tableAlias>.opt_a_b1
  remapFields: (frag, fields, scalar, tableAlias) ->
    if not frag or not frag.type
      return frag

    switch frag.type
      when "field"
        for field in fields
          # Remap
          if field.tableAlias == frag.tableAlias and field.column == frag.column
            return { type: "field", tableAlias: tableAlias, column: "opt_#{field.tableAlias}_#{field.column}" }
        return frag
      when "op"
        return _.extend({}, frag, exprs: _.map(frag.exprs, (ex) => @remapFields(ex, fields, scalar, tableAlias)))
      when "case"
        return _.extend({}, frag, {
          input: @remapFields(frag.input, fields, scalar, tableAlias)
          cases: _.map(frag.cases, (cs) =>
            {
              when: @remapFields(cs.when, fields, scalar, tableAlias)
              then: @remapFields(cs.then, fields, scalar, tableAlias)
            }
          )
          else: @remapFields(frag.else, fields, scalar, tableAlias)
        })
      when "scalar"
        if scalar == frag
          return { type: "field", tableAlias: tableAlias, column: "expr" }
        else 
          return _.extend({}, frag, {
            frag: @remapFields(frag.frag, fields, scalar, tableAlias)
            from: @remapFields(frag.from, fields, scalar, tableAlias)
            where: @remapFields(frag.where, fields, scalar, tableAlias)
            orderBy: @remapFields(frag.orderBy, fields, scalar, tableAlias)
          })
      when "table"
        return frag
      when "join"
        return _.extend({}, frag, {
          left: @remapFields(frag.left, fields, scalar, tableAlias)
          right: @remapFields(frag.right, fields, scalar, tableAlias)
          on: @remapFields(frag.on, fields, scalar, tableAlias)
        })
      when "literal"
        return frag
      when "token"
        return frag
      else
        throw new Error("Unsupported remapFields with type #{frag.type}")

