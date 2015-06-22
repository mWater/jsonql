LookupSchemaCompiler = require './LookupSchemaCompiler'
fs = require 'fs'

# Call the compile function and log the response, so that PHP can pick that up
LookupSchemaCompiler.compile(process.argv, fs.readFile, (response)->
  console.log response
  process.exit())
