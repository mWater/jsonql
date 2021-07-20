export default class QueryOptimizer {
    aliasNum: number;
    constructor();
    debugQuery(query: any): void;
    optimizeQuery(query: any, debug?: boolean): any;
    rewriteScalar(query: any): any;
    optimizeInnerQueries(query: any): any;
    findScalar(frag: any): any;
    changeAlias(frag: any, fromAlias: any, toAlias: any): any;
    extractFromAliases(from: any): any;
    extractFields: (frag: any) => any;
    isAggr: (expr: any) => any;
    remapFields(frag: any, fields: any, scalar: any, tableAlias: any): any;
    createAlias(): string;
}
