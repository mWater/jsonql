var JsonqlCompiler, LookupSchemaCompiler, LookupSchemaMap, tryParse;

LookupSchemaMap = require('./LookupSchemaMap');

JsonqlCompiler = require('./JsonqlCompiler');

tryParse = function(input) {
  var err, object;
  try {
    object = JSON.parse(input);
    err = null;
    return [err, object];
  } catch (_error) {
    err = _error;
    object = null;
    return [err, object];
  }
};

module.exports = LookupSchemaCompiler = (function() {
  function LookupSchemaCompiler() {}

  LookupSchemaCompiler.compile = function(processArgs, readFile, callback) {
    var queryStr, respond, respondError, tableMapFilename;
    respond = function(response) {
      var responseStr;
      responseStr = JSON.stringify(response);
      return callback(responseStr);
    };
    respondError = function(message) {
      return respond({
        error: message
      });
    };
    if (processArgs.length < 3) {
      respondError("Compile process was called without schema document file");
      return;
    }
    if (processArgs.length < 4) {
      respondError("Compile process was called without JSON input");
      return;
    }
    tableMapFilename = processArgs[2];
    queryStr = processArgs[3];
    return readFile(tableMapFilename, function(err, tableMapStr) {
      var compiler, query, ref, ref1, response, schemaMap, tableMap;
      if (err) {
        respondError("Error reading " + tableMapFilename + ": " + err);
        return;
      }
      ref = tryParse(tableMapStr), err = ref[0], tableMap = ref[1];
      if (err) {
        respondError("Error parsing " + tableMapFilename + ": " + err);
        return;
      }
      ref1 = tryParse(queryStr), err = ref1[0], query = ref1[1];
      if (err) {
        respondError("Error parsing input query: " + err);
        return;
      }
      schemaMap = new LookupSchemaMap(tableMap);
      compiler = new JsonqlCompiler(schemaMap);
      response = (function() {
        try {
          query = compiler.compileQuery(query);
          return {
            query: query
          };
        } catch (_error) {
          err = _error;
          return {
            error: "Error compiling query: " + err
          };
        }
      })();
      return respond(response);
    });
  };

  return LookupSchemaCompiler;

})();
