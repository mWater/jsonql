LookupSchemaMap = require './LookupSchemaMap'
JsonqlCompiler = require './JsonqlCompiler'

# Helper function to try to parse a JSON input, and return a list [err, parsedObject]
tryParse = (input) ->
  try 
    object = JSON.parse input
    err = null
    [err, object]
  catch err 
    object = null
    [err, object]

# Compiler for LookupSchema.  One static function
module.exports = class LookupSchemaCompiler

  # Compiles the query based on expected args:
  # processArgs: [_, _, tableMapFilename, jsonQueryString]
  # readFile: Just an IoC for fs.readFile
  # callback: Handler for calling back the script with the result
  # Result will be a JSON-formatted string for {query: {sql: "_", params: []}} or {error: msg}
  @compile: (processArgs, readFile, callback) ->
  
    # Helper functions
    respond = (response) -> 
      responseStr = JSON.stringify response
      callback responseStr
    respondError = (message) -> respond {error: message}
    
    # Validate processArgs and get values
    if processArgs.length < 3
      respondError("Compile process was called without schema document file")
      return      
    if processArgs.length < 4 
      respondError("Compile process was called without JSON input")
      return
    tableMapFilename = processArgs[2]
    queryStr = processArgs[3]
    
    # Read the file asynchronously, with callback block
    readFile(tableMapFilename, (err, tableMapStr) ->
    
      # Validate and parse the table map file contents and the passed-in query
      if err
        respondError("Error reading #{tableMapFilename}: #{err}")
        return
      [err, tableMap] = tryParse tableMapStr
      if err
        respondError("Error parsing #{tableMapFilename}: #{err}")
        return
      [err, query] = tryParse queryStr
      if err
        respondError("Error parsing input query: #{err}")
        return
        
      # Compile the query
      schemaMap = new LookupSchemaMap(tableMap)
      compiler = new JsonqlCompiler(schemaMap)
      response = 
        try 
          query = compiler.compileQuery query
          {query: query}
        catch err
          {error: "Error compiling query: #{err}"}
      respond response)
