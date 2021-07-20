// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
let SqlFragment;
import _ from 'lodash';

// Fragment of SQL that has sql (text) and params (array)
export default SqlFragment = class SqlFragment { 
  constructor(sql, params) {
    this.sql = sql || "";
    this.params = params || [];
  }

  // Append a string (just sql), [sql, params], SqlFragment or plain object (has sql and params)
  append(val, params) {
    if (_.isString(val)) {
      this.sql += val;
      this.params = this.params.concat(params || []);
    } else {
      this.sql += val.sql;
      this.params = this.params.concat(val.params);
    }

    return this;
  }

  isEmpty() {
    return this.sql.length === 0;
  }

  static join(list, joiner) {
    return new SqlFragment(_.map(list, fr => fr.sql).join(joiner), [].concat.apply([], _.pluck(list, "params")));
  }
    
  // Make into sql with parameters inlined
  toInline() {
    // Escapes a literal value
    function escapeLiteral(val) {
      if (val === null) {
        return "null";
      }

      if (typeof(val) === "string") {
        return escapeString(val);
      }

      if (typeof(val) === "number") {
        return "" + val;
      }

      if (typeof(val) === "boolean") {
        if (val) { return "TRUE"; } else { return "FALSE"; }
      }

      if (_.isArray(val)) {
        return "array[" + _.map(val, escapeLiteral).join(',') + "]";
      }

      if (val instanceof Date) {
        return escapeString(val.toISOString());
      }

      if (typeof(val) === "object") {
        return "(" + escapeString(JSON.stringify(val)) + "::json)";
      }

      throw new Error("Unsupported literal value: " + val);
    }

    // Substitute parameters
    let n = 0;
    // All the question marks not followed by | or &
    // ?| and ?& are jsonb operators (so is ?, but it can be replaced by one of the others)
    const sql = this.sql.replace(/\?(?!\||&)/g, str => {
      // Insert nth parameter
      // Check type
      const param = this.params[n];
      n += 1;
      return escapeLiteral(param);
    });

    return sql;
  }
};

function escapeString(val) {
  const backslash = ~val.indexOf('\\');
  const prefix = backslash ? 'E' : '';
  val = val.replace(/'/g, '\'\'');
  val = val.replace(/\\/g, '\\\\');
  return prefix + '\'' + val + '\'';
}
  