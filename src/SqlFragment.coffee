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
    # Escapes a literal value
    escapeLiteral = (val) ->
      if val == null
        return "null"

      if typeof(val) == "string"
        return pgescape.literal(val)

      if typeof(val) == "number"
        return "" + val

      if typeof(val) == "boolean"
        return if val then "TRUE" else "FALSE"

      if _.isArray(val)
        return "array[" + _.map(val, escapeLiteral).join(',') + "]"

      if typeof(val) == "object"
        return "(" + pgescape.literal(JSON.stringify(val)) + "::json)"

      throw new Error("Unsupported literal value: " + val)

    # Substitute parameters
    n = 0
    # All the question marks not followed by | or &
    # ?| and ?& are jsonb operators (so is ?, but it can be replaced by one of the others)
    sql = @sql.replace(/\?(?!\||&)/g, (str) =>
      # Insert nth parameter
      # Check type
      param = @params[n]
      n += 1
      return escapeLiteral(param)
    )

    return sql

