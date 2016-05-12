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

  constructor: ->
    # Next table alias number
    @aliasNum = 0

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

      if _.isEqual(optQuery, query)
        return optQuery

      if debug
        console.log "================== OPT #{i} ================"
        @debugQuery(optQuery)

      query = optQuery

    throw new Error("Unable to optimize query (infinite loop): #{JSON.stringify(query)}")

  rewriteScalar: (query) ->
    # First optimize any inner queries
    query = @optimizeInnerQueries(query)

    # Find scalar to optimize
    scalar = @findScalar(query)

    # If no scalar to optimize, return
    if not scalar
      return query

    # If scalar doesn't have simply aliases from, return
    if not scalar.from.alias
      return query

    oldScalarAlias = scalar.from.alias
    newScalarAlias = @createAlias()

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
      # Re-write query selects to use new opt1 query
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
      # Create new selects for opt1 query with all fields + scalar expression
      opt1Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt1Selects.push({ type: "select", expr: @changeAlias(scalar.expr, oldScalarAlias, newScalarAlias), alias: "expr" })

      # Create new opt1 from clause with left outer join to scalar
      opt1From = { 
        type: "join"
        kind: "left"
        left: query.from
        right: @changeAlias(scalar.from, oldScalarAlias, newScalarAlias)
        on: @changeAlias(scalar.where, oldScalarAlias, newScalarAlias) 
      }

      # Create opt1 query opt1
      opt1Query = {
        type: "query"
        selects: opt1Selects
        from: opt1From
        where: innerWhere
      }

      # Optimize inner query
      opt1Query = @optimizeQuery(opt1Query, false)

      # Create alias for opt1 query
      opt1Alias = @createAlias()

      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt1 query
        selects: remapSelects(query.selects, opt1Alias)
        from: {
          type: "subquery"
          query: opt1Query
          alias: opt1Alias
        }
        where: @remapFields(outerWhere, fields, scalar, opt1Alias)
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, opt1Alias) })
        )
      })
      return outerQuery

    else if not scalar.limit
      # Create new selects for opt1 query with all fields + row number
      opt1Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt1Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" })

      # Create alias for opt1 query
      opt1Alias = @createAlias()

      # Create opt1 query opt1
      opt1Query = {
        type: "query"
        selects: opt1Selects
        from: query.from
        where: innerWhere
      }

      # Optimize inner query
      opt1Query = @optimizeQuery(opt1Query, false)

      # Create new selects for opt2 query with row number + all fields + scalar expression
      opt2Selects = [{ type: "select", expr: { type: "field", tableAlias: opt1Alias, column: "rn" }, alias: "rn" }]
      opt2Selects = opt2Selects.concat(_.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: opt1Alias, column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      ))
      opt2Selects.push({ type: "select", expr: @changeAlias(@remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias), alias: "expr" })

      # Create new opt2 from clause with left outer join to scalar
      opt2From = { 
        type: "join"
        kind: "left"
        left: { type: "subquery", query: opt1Query, alias: opt1Alias }
        right: @changeAlias(scalar.from, oldScalarAlias, newScalarAlias)
        on: @changeAlias(@remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
      }

      opt2Query = {
        type: "query"
        selects: opt2Selects
        from: opt2From
        groupBy: _.range(1, fields.length + 2)
      }

      # Create alias for opt2 query
      opt2Alias = @createAlias()

      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt2 query
        selects: remapSelects(query.selects, opt2Alias)
        from: {
          type: "subquery"
          query: opt2Query
          alias: opt2Alias
        }
        where: @remapFields(outerWhere, fields, scalar, opt2Alias)
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, opt2Alias) })
        )
      })

      return outerQuery

    # Limit scalar
    else 
      # Create new selects for opt1 query with all fields + row number
      opt1Selects = _.map(fields, (field) =>
        { type: "select", expr: field, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt1Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {}, alias: "rn" })

      # Create opt1 query opt1
      opt1Query = {
        type: "query"
        selects: opt1Selects
        from: query.from
        where: innerWhere
      }

      # Optimize inner query (TODO give each unique name?)
      opt1Query = @optimizeQuery(opt1Query, false)

      # Create alias for opt1 query
      opt1Alias = @createAlias()

      # Create new selects for opt2 query with all fields + scalar expression + ordered row number over inner row number
      opt2Selects = _.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: opt1Alias, column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt2Selects.push({ type: "select", expr: @changeAlias(@remapFields(scalar.expr, fields, null, opt1Alias), oldScalarAlias, newScalarAlias), alias: "expr" })
      opt2Selects.push({ type: "select", expr: { type: "op", op: "row_number", exprs: [] }, over: {
        partitionBy: [{ type: "field", tableAlias: opt1Alias, column: "rn" }]
        orderBy: _.map(scalar.orderBy, (ob) => 
          if ob.expr
            return _.extend({}, ob, expr: @changeAlias(ob.expr, oldScalarAlias, newScalarAlias))
          return ob
        )
      }, alias: "rn" })

      # Create new opt2 from clause with left outer join to scalar
      opt2From = { 
        type: "join"
        kind: "left"
        left: { type: "subquery", query: opt1Query, alias: opt1Alias }
        right: @changeAlias(scalar.from, oldScalarAlias, newScalarAlias)
        on: @changeAlias(@remapFields(scalar.where, fields, scalar, opt1Alias), oldScalarAlias, newScalarAlias)
      }

      opt2Query = {
        type: "query"
        selects: opt2Selects
        from: opt2From
      }

      # Create alias for opt2 query
      opt2Alias = @createAlias()

      opt3Selects = _.map(fields, (field) =>
        { type: "select", expr: { type: "field", tableAlias: opt2Alias, column: "opt_#{field.tableAlias}_#{field.column}" }, alias: "opt_#{field.tableAlias}_#{field.column}" }
      )
      opt3Selects.push({ type: "select", expr: { type: "field", tableAlias: opt2Alias, column: "expr" }, alias: "expr" })

      opt3Query = {
        type: "query"
        selects: opt3Selects
        from: {
          type: "subquery"
          query: opt2Query
          alias: opt2Alias
        }
        where: {
          type: "op"
          op: "="
          exprs: [
            { type: "field", tableAlias: opt2Alias, column: "rn" }
            { type: "literal", value: 1 }
          ]
        }
      }

      # Create alias for opt3 query
      opt3Alias = @createAlias()

      # Wrap in final query
      outerQuery = _.extend({}, query, {
        # Re-write query selects to use new opt2 query
        selects: remapSelects(query.selects, opt3Alias)
        from: {
          type: "subquery"
          query: opt3Query
          alias: opt3Alias
        }
        where: @remapFields(outerWhere, fields, scalar, opt3Alias)
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, opt3Alias) })
        )
      })

      return outerQuery

  optimizeInnerQueries: (query) ->
    optimizeFrom = (from) =>
      switch from.type
        when "table", "subexpr"
          return from
        when "join"
          return _.extend({}, from, {
            left: optimizeFrom(from.left)
            right: optimizeFrom(from.right)
          })
        when "subquery"
          return _.extend({}, from, {
            query: @optimizeQuery(from.query)
          })
        else
          throw new Error("Unknown optimizeFrom type #{from.type}")

    query = _.extend({}, query, from: optimizeFrom(query.from))

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

  # replaceFrag: (frag, fromFrag, toFrag) ->
  #   if not frag or not frag.type
  #     return frag

  #   if frag == from
  #     return to

  #   switch frag.type
  #     when "query"
  #       return _.extend({}, frag,
  #         selects: _.map(frag.selects, (ex) => @replaceFrag(ex, fromFrag, toFrag)))
  #         from: @replaceFrag(frag.from, fromFrag, toFrag)
  #         where: @replaceFrag(frag.where, fromFrag, toFrag)
  #         orderBy: @replaceFrag(frag.where, fromFrag, toFrag)
  #         )

  #     when "field"
  #       return frag
  #     when "op"
  #       return _.extend({}, frag, exprs: _.map(frag.exprs, (ex) => @replaceFrag(ex, fromFrag, toFrag)))
  #     when "case"
  #       return _.extend({}, frag, {
  #         input: @replaceFrag(frag.input, fromFrag, toFrag)
  #         cases: _.map(frag.cases, (cs) =>
  #           {
  #             when: @replaceFrag(cs.when, fromFrag, toFrag)
  #             then: @replaceFrag(cs.then, fromFrag, toFrag)
  #           }
  #         )
  #         else: @replaceFrag(frag.else, fromFrag, toFrag)
  #       })
  #     when "scalar"
  #       return _.extend({}, frag, {
  #         expr: @replaceFrag(frag.expr, fromFrag, toFrag)
  #         from: @replaceFrag(frag.from, fromFrag, toFrag)
  #         where: @replaceFrag(frag.where, fromFrag, toFrag)
  #         orderBy: @replaceFrag(frag.orderBy, fromFrag, toFrag)
  #       })
  #     when "table"
  #       if frag.alias == fromFrag
  #         return { type: "table", table: frag.table, alias: toFrag }
  #       return frag
  #     when "join"
  #       return _.extend({}, frag, {
  #         left: @replaceFrag(frag.left, fromFrag, toFrag)
  #         right: @replaceFrag(frag.right, fromFrag, toFrag)
  #         on: @replaceFrag(frag.on, fromFrag, toFrag)
  #       })
  #     when "literal"
  #       return frag
  #     when "token"
  #       return frag
  #     else
  #       throw new Error("Unsupported replaceFrag with type #{frag.type}")

  # Change a specific alias to another one
  changeAlias: (frag, fromAlias, toAlias) ->
    if not frag or not frag.type
      return frag

    switch frag.type
      when "field"
        if frag.tableAlias == fromAlias
          # Remap
          return { type: "field", tableAlias: toAlias, column: frag.column }
        return frag
      when "op"
        return _.extend({}, frag, exprs: _.map(frag.exprs, (ex) => @changeAlias(ex, fromAlias, toAlias)))
      when "case"
        return _.extend({}, frag, {
          input: @changeAlias(frag.input, fromAlias, toAlias)
          cases: _.map(frag.cases, (cs) =>
            {
              when: @changeAlias(cs.when, fromAlias, toAlias)
              then: @changeAlias(cs.then, fromAlias, toAlias)
            }
          )
          else: @changeAlias(frag.else, fromAlias, toAlias)
        })
      when "scalar"
        newFrag = _.extend({}, frag, {
          expr: @changeAlias(frag.expr, fromAlias, toAlias)
          from: @changeAlias(frag.from, fromAlias, toAlias)
          where: @changeAlias(frag.where, fromAlias, toAlias)
          orderBy: @changeAlias(frag.orderBy, fromAlias, toAlias)
        })
        if frag.orderBy
          newFrag.orderBy = _.map(frag.orderBy, (ob) => 
            if ob.expr
              return _.extend({}, ob, expr: @changeAlias(ob.expr, fromAlias, toAlias))
            return ob
          )
        return newFrag

      when "table"
        if frag.alias == fromAlias
          return { type: "table", table: frag.table, alias: toAlias }
        return frag
      when "join"
        return _.extend({}, frag, {
          left: @changeAlias(frag.left, fromAlias, toAlias)
          right: @changeAlias(frag.right, fromAlias, toAlias)
          on: @changeAlias(frag.on, fromAlias, toAlias)
        })
      when "literal"
        return frag
      when "token"
        return frag
      else
        throw new Error("Unsupported changeAlias with type #{frag.type}")

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
          newFrag = _.extend({}, frag, {
            expr: @remapFields(frag.expr, fields, scalar, tableAlias)
            from: @remapFields(frag.from, fields, scalar, tableAlias)
            where: @remapFields(frag.where, fields, scalar, tableAlias)
          })
          if frag.orderBy
            newFrag.orderBy = _.map(frag.orderBy, (ob) => 
              if ob.expr
                return _.extend({}, ob, expr: @remapFields(ob.expr, fields, scalar, tableAlias))
              return ob
            )
          return newFrag

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

  # Create a unique table alias
  createAlias: ->
    alias = "opt#{@aliasNum}"
    @aliasNum += 1
    return alias
