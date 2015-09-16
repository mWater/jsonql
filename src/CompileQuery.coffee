minimist = require('minimist')
yaml = require('js-yaml')
fs = require('fs')
LookupSchemaMap = require './LookupSchemaMap'
JsonqlCompiler = require './JsonqlCompiler'

# How to indicate error
respondError = (message) -> 
  console.log(JSON.stringify({error: message}))

# Parse arguments
args = require('minimist')(process.argv.slice(2))

schemaPath = args._[0]
queryStr = args._[1]

if not schemaPath
  return respondError("Compile process was called without schema document file")
if not queryStr
  return respondError("Compile process was called without JSON input")

# Read schema file
try 
  schemaStr = fs.readFileSync(schemaPath, 'utf8')
catch err
  return respondError("Cannot load file #{schemaPath}: #{err}")

# Parse schema file
try 
  if schemaPath.match(/.yaml$/)
    schemaJson = yaml.safeLoad(schemaStr)
  else
    schemaJson = JSON.parse(schemaStr)    
catch err
  return respondError("Cannot parse file #{schemaPath}: #{err}")

# Parse query
try 
  query = JSON.parse(queryStr)
catch err
  return respondError("Cannot parse query: #{err}")

# Compile the query
schemaMap = new LookupSchemaMap(schemaJson)
compiler = new JsonqlCompiler(schemaMap)

try 
  result = compiler.compileQuery(query)
  # Inline query to have better control over sql
  console.log(JSON.stringify({query: result.toInline(), params: []}))
catch err
  return respondError("Cannot compile query: #{err}")
