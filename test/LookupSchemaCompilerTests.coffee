assert = require('chai').assert
LookupSchemaCompiler = require '../src/LookupSchemaCompiler'

abcTableMap = {
  tables: [ { "id": "abc", "sql": "ABC", "columns": [
      { "id": "p", "sql": "{alias}.P" }]}]
  }
      
abcQuery = { 
  type: "query"
  selects: [
    { type: "select", expr: { type: "field", tableAlias: "abc1", column: "p" }, alias: "x" }
  ]
  from: { type: "table", table: "abc", alias: "abc1" }
}

### Mocks for fs.readFile, which has signature (filename, callback), where callback is called with (err, data) ###

# Create a mock fs.readFile that always calls back with error message "err"
mockFileReaderWithError = (err) ->
  (filename, callback) -> callback(err, null)
  
# Create a mock fs.readFile that always calls back with file contents "contents"
mockFileReaderForContents = (contents) ->
  (filename, callback) -> callback(null, contents) 
  
# Create a mock fs.readFile that always calls back with the stringified table map
mockFileReaderForTable = (tableMap) ->
  tableMapStr = JSON.stringify tableMap
  mockFileReaderForContents tableMapStr
  
  
### Mocks for input process.argv ###
  
# Create process args PHP would send for this query string
# (the first two process args are unused, and the tableMapFilename can be ignored because fs.readFile is mocked)
mockProcessArgsForQueryStr = (queryStr) -> [null, null, null, queryStr]

# Create process args PHP would send for the query object
mockProcessArgsForQuery = (query) ->
  queryStr = JSON.stringify query
  mockProcessArgsForQueryStr queryStr
  
  
### Testing Skeletons ###

# Skeleton for a test method that expects a valid SQL query to be returned from the input mocks
compileExpectSuccess = (processArgs, readFile, expectedQuery, done) ->
  LookupSchemaCompiler.compile(processArgs, readFile, (response) ->
    parsedResponse = JSON.parse response
    assert.deepEqual parsedResponse.query, expectedQuery
    done())

# Skeleton for a test method that expects a valid SQL query to be returned from the inputs
runExpectSuccess = (query, tableMap, expectedQuery, done) ->
  # Create our mocks
  processArgs = mockProcessArgsForQuery query
  readFile = mockFileReaderForTable abcTableMap
  compileExpectSuccess(processArgs, readFile, expectedQuery, done)
    
# Skeleton for a test method that expects an error to be returned from the input mocks
compileExpectError = (processArgs, readFile, done) ->
  LookupSchemaCompiler.compile(processArgs, readFile, (response) ->
    # Validate response
    parsedResponse = JSON.parse response
    assert.property parsedResponse, 'error'
    done())
    
# Skeleton for a test method that expects an error to be returned from the inputs
runExpectError = (query, tableMap, done) ->
  processArgs = mockProcessArgsForQuery query
  readFile = mockFileReaderForTable tableMap
  compileExpectError(processArgs, readFile, done)
  
  
### LookupSchemaCompiler Tests ###
describe "LookupSchemaCompiler", ->

  it 'compiles query with field', (done) ->
    expectedResult = {sql: 'select alias_abc1.P as "x" from ABC as "alias_abc1"', params: []}
    runExpectSuccess(abcQuery, abcTableMap, expectedResult, done)
    
  it 'replies with error on bad query', (done) ->
    query = { 
      type: "query"
      selects: [
        { type: "select", expr: { type: "field", tableAlias: "does_not_exist", column: "p" }, alias: "x" }
      ]
      from: { type: "table", table: "abc", alias: "abc1" }
    }
    runExpectError(query, abcTableMap, done)
    
  it 'replies with error if file does not exist', (done) ->
    # Create our mocks
    processArgs = mockProcessArgsForQuery abcQuery
    readFile = mockFileReaderWithError "BOOM"
    compileExpectError(processArgs, readFile, done)
