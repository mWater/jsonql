"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const SqlFragment_1 = __importDefault(require("./SqlFragment"));
/** Maps tables and columns to a secure sql fragment. Base class is simple passthrough */
class SchemaMap {
    /** Maps a table to a secured, sanitized version */
    mapTable(table) {
        return new SqlFragment_1.default(table);
    }
    /** Map a column reference of a table aliased as escaped alias alias */
    mapColumn(table, column, alias) {
        return new SqlFragment_1.default(alias + "." + column);
    }
    /** Escapes a table alias. Should prefix with alias_ or similar for security */
    mapTableAlias(alias) {
        return alias;
    }
}
exports.default = SchemaMap;
