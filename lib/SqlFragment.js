"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
/** Fragment of SQL that has sql (text) and params (array) */
class SqlFragment {
    constructor(sql, params) {
        this.sql = sql || "";
        this.params = params || [];
    }
    append(val, params) {
        if (lodash_1.default.isString(val)) {
            this.sql += val;
            this.params = this.params.concat(params || []);
        }
        else {
            this.sql += val.sql;
            this.params = this.params.concat(val.params);
        }
        return this;
    }
    isEmpty() {
        return this.sql.length === 0;
    }
    static join(list, joiner) {
        return new SqlFragment(lodash_1.default.map(list, (fr) => fr.sql).join(joiner), [].concat.apply([], lodash_1.default.pluck(list, "params")));
    }
    /** Make into sql with parameters inlined */
    toInline() {
        // Escapes a literal value
        function escapeLiteral(val) {
            if (val === null) {
                return "null";
            }
            if (typeof val === "string") {
                return escapeString(val);
            }
            if (typeof val === "number") {
                return "" + val;
            }
            if (typeof val === "boolean") {
                if (val) {
                    return "TRUE";
                }
                else {
                    return "FALSE";
                }
            }
            if (lodash_1.default.isArray(val)) {
                return "array[" + lodash_1.default.map(val, escapeLiteral).join(",") + "]";
            }
            if (val instanceof Date) {
                return escapeString(val.toISOString());
            }
            if (typeof val === "object") {
                return "(" + escapeString(JSON.stringify(val)) + "::json)";
            }
            throw new Error("Unsupported literal value: " + val);
        }
        // Substitute parameters
        let n = 0;
        // All the question marks not followed by | or &
        // ?| and ?& are jsonb operators (so is ?, but it can be replaced by one of the others)
        // This complex expression is to not match 'xyz?'. See https://stackoverflow.com/questions/6462578/regex-to-match-all-instances-not-inside-quotes
        const sql = this.sql.replace(/\?(?!\||&)(?=([^']*'[^']*')*[^']*$)/g, (str) => {
            // Insert nth parameter
            // Check type
            const param = this.params[n];
            n += 1;
            return escapeLiteral(param);
        });
        return sql;
    }
}
exports.default = SqlFragment;
function escapeString(val) {
    const backslash = ~val.indexOf("\\");
    const prefix = backslash ? "E" : "";
    val = val.replace(/'/g, "''");
    val = val.replace(/\\/g, "\\\\");
    return prefix + "'" + val + "'";
}
