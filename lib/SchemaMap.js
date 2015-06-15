var SchemaMap, SqlFragment;

SqlFragment = require('./SqlFragment');

module.exports = SchemaMap = (function() {
  function SchemaMap() {}

  SchemaMap.prototype.mapTable = function(table) {
    return new SqlFragment(table);
  };

  SchemaMap.prototype.mapColumn = function(table, column, alias) {
    return new SqlFragment(alias + "." + column);
  };

  SchemaMap.prototype.mapTableAlias = function(alias) {
    return alias;
  };

  return SchemaMap;

})();
