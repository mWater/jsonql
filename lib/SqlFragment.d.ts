/** Fragment of SQL that has sql (text) and params (array) */
export default class SqlFragment {
  sql: string
  params: any[]
  
  constructor(sql: string, params?: any[])

  /** Append a string (just sql), [sql, params], SqlFragment or plain object (has sql and params) */
  append(val: string, params?: any[]): SqlFragment
  append(val: SqlFragment): SqlFragment
  append(val: [string, any[]]): SqlFragment
  append(val: { sql: string, params: any[] }): SqlFragment

  isEmpty(): boolean

  static join(list: SqlFragment[], joiner: string): SqlFragment
    
  /** Make into sql with parameters inlined */
  toInline(): string
}