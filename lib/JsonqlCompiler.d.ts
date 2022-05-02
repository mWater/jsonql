import SqlFragment from "./SqlFragment";
import { JsonQLQuery, JsonQLExpr } from ".";
import SchemaMap from "./SchemaMap";
/** Compiles jsonql to sql */
export default class JsonqlCompiler {
    optimizeQueries: boolean;
    schemaMap: SchemaMap;
    nextId: number;
    constructor(schemaMap: SchemaMap, optimizeQueries?: boolean);
    compileQuery(query: JsonQLQuery, aliases?: {
        [alias: string]: string | true;
    }, ctes?: {
        [alias: string]: boolean;
    }): Promise<SqlFragment>;
    compileSelect(select: any, aliases: any, ctes?: {}): Promise<SqlFragment>;
    compileFrom(from: any, aliases?: {}, ctes?: {}): Promise<SqlFragment>;
    compileOrderBy(orderBy: any, aliases: any): Promise<SqlFragment>;
    /** Compiles an expression
     aliases are dict of unmapped alias to table name, or true whitelisted tables (CTEs and subqueries and subexpressions)
     */
    compileExpr(expr: JsonQLExpr, aliases: {
        [alias: string]: string | true;
    }, ctes?: {
        [alias: string]: boolean;
    }): Promise<SqlFragment>;
    compileOpExpr(expr: any, aliases: any, ctes?: {}): Promise<SqlFragment>;
    compileScalar(query: any, aliases: any, ctes?: {}): Promise<SqlFragment>;
    compileCaseExpr(expr: any, aliases: any, ctes?: {}): Promise<SqlFragment>;
    validateAlias(alias: any): void;
}
