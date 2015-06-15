_ = require 'lodash'
pgescape = require 'pg-escape'

# Fragment of SQL that has sql (text) and params (array)
module.exports = class SqlFragment 
  constructor: (sql, params) ->
    @sql = sql or ""
    @params = params or []

  # Append a string (just sql), [sql, params], SqlFragment or plain object (has sql and params)
  append: (val, params) ->
    if _.isString(val)
      @sql += val
      @params = @params.concat(params or [])
    else
      @sql += val.sql
      @params = @params.concat(val.params)

    return this

  isEmpty: ->
    return @sql.length == 0

  @join: (list, joiner) ->
    return new SqlFragment(_.map(list, (fr) -> fr.sql).join(joiner), [].concat.apply([], _.pluck(list, "params")))
    
  # Make into sql with parameters inlined
  toInline: ->
    # Substitute parameters
    n = 0
    sql = @sql.replace(/\?/g, (str) =>
      # Insert nth parameter
      # Check type
      param = @params[n]
      n += 1

      if param == null
        return "null"

      if typeof(param) == "string"
        return pgescape.literal(param)

      if typeof(param) == "number"
        return "" + param

      if _.isArray(param)
        return "array[" + _.map(param, (p) -> pgescape.literal(p)).join(',') + "]"

      if typeof(param) == "object"
        return "(" + pgescape.literal(JSON.stringify(param)) + "::json)"

      throw new Error("Unsupported parameter: " + param)
    )

    return sql

