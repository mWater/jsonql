"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const SqlFragment_1 = __importDefault(require("./SqlFragment"));
/** Maps tables and columns to a secure sql fragment. Base class is simple passthrough */
class SchemaMap {
    /** Maps a table to a secured, sanitized version */
    mapTable(table) {
        return __awaiter(this, void 0, void 0, function* () {
            return new SqlFragment_1.default(table);
        });
    }
    /** Map a column reference of a table aliased as escaped alias alias */
    mapColumn(table, column, alias) {
        return __awaiter(this, void 0, void 0, function* () {
            return new SqlFragment_1.default(alias + "." + column);
        });
    }
    /** Escapes a table alias. Should prefix with alias_ or similar for security */
    mapTableAlias(alias) {
        return alias;
    }
}
exports.default = SchemaMap;
