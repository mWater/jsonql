var LookupSchemaCompiler, fs;

LookupSchemaCompiler = require('./LookupSchemaCompiler');

fs = require('fs');

LookupSchemaCompiler.compile(process.argv, fs.readFile, function(response) {
  console.log(response);
  return process.exit();
});
