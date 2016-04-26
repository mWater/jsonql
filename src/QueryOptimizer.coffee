_ = require 'lodash'
###

Scalar subqueries can be very slow in Postgresql as they are not re-written but instead loop over and over.

This attempts to re-write them as left outer joins, which is a complex tranformation


###


module.exports = class QueryOptimizer
  optimizeQuery: (query) ->
    # Do not use if having clause
    if query.having
      return query

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
        where: query.where  # TODO where is completely in opt0 query
      }

      outerQuery = {
        type: "query"
        # Re-write query selects to use new opt0 query
        selects: _.map(query.selects, (select) =>
          {
            type: "select"
            expr: @remapFields(select.expr, fields, scalar, "opt0")
            alias: select.alias
          }
        )
        from: {
          type: "subquery"
          query: opt0Query
          alias: "opt0"
        }
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt0") })
        )
      }
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
        where: query.where  # TODO where is completely in opt0 query
      }

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

      outerQuery = {
        type: "query"
        # Re-write query selects to use new opt1 query
        selects: _.map(query.selects, (select) =>
          {
            type: "select"
            expr: @remapFields(select.expr, fields, scalar, "opt1")
            alias: select.alias
          }
        )
        from: {
          type: "subquery"
          query: opt1Query
          alias: "opt1"
        }
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt1") })
        )
      }

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
        where: query.where  # TODO where is completely in opt0 query
      }

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
      outerQuery = {
        type: "query"
        # Re-write query selects to use new opt1 query
        selects: _.map(query.selects, (select) =>
          {
            type: "select"
            expr: @remapFields(select.expr, fields, scalar, "opt2")
            alias: select.alias
          }
        )
        from: {
          type: "subquery"
          query: opt2Query
          alias: "opt2"
        }
        orderBy: _.map(query.orderBy, (orderBy) =>
          if not orderBy.expr
            return orderBy
          return _.extend({}, orderBy, { expr: @remapFields(orderBy.expr, fields, scalar, "opt2") })
        )
      }

      return outerQuery





    # TODO put as much of where clause in inner query but not scalars 



  # Find a scalar in where, selects or order by or expression
  findScalar: (frag) ->
    if not frag
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
      else
        throw new Error("Unsupported isAggr with type #{expr.type}")

  # Remap fields a.b1 to format <tableAlias>.opt_a_b1
  remapFields: (expr, fields, scalar, tableAlias) ->
    if not expr or not expr.type
      return expr

    switch expr.type
      when "field"
        for field in fields
          # Remap
          if field == expr
            return { type: "field", tableAlias: tableAlias, column: "opt_#{field.tableAlias}_#{field.column}" }
        return expr
      when "op"
        return _.extend({}, expr, exprs: _.map(expr.exprs, (ex) => @remapFields(ex, fields, scalar, tableAlias)))
      when "case"
        return _.extend({}, expr, {
          input: @remapFields(expr.input, fields, scalar, tableAlias)
          cases: _.map(expr.cases, (cs) =>
            {
              when: @remapFields(cs.when, fields, scalar, tableAlias)
              then: @remapFields(cs.then, fields, scalar, tableAlias)
            }
          )
          else: @remapFields(expr.else, fields, scalar, tableAlias)
        })
      when "scalar"
        if scalar == expr
          return { type: "field", tableAlias: tableAlias, column: "expr" }
        return expr
      when "literal"
        return expr
      else
        throw new Error("Unsupported remapFields with type #{expr.type}")

