<?php

/**
 * Queries the database based on a JsonQL input
 * @param array $jqlQry The JsonQL query represented as an array
 * @param string $tableMapFile The path where the table map file is located
 * @return array Either ["json" => jsonEncodedString] or ["error" => errorMsg]
 */
function query($jqlQry, $tableMapFile, $con) {
  $projectDir = dirname(dirname(__FILE__));
  $compileJs = "$projectDir/lib/CompileQuery.js";

  # Execute the JsonQL transform in Node
  $jqlJson = json_encode($jqlQry);
  $jqlJsonArg = escapeshellarg($jqlJson);
  $tableMapFileArg = escapeShellArg($tableMapFile);
  $cmd = "/usr/bin/nodejs $compileJs $tableMapFileArg $jqlJsonArg";
  $output = exec($cmd, $outputArray, $returnCode);
  
  # Return early if error
  if ($returnCode != 0) {
    return ["error"=>"Compiler returned error code $returnCode"];
  }
  $outputObj = json_decode($output);
  if ($outputObj == null || (!property_exists($outputObj, "query") && !property_exists($outputObj, "error"))) {
    return ["error"=>"Compiler did not complete successfully"];
  }
  if (property_exists($outputObj, "error")) {
    return ["error" => $outputObj->error];
  }

  # Substitute the ? args with $N'S in the query
  $queryObj = $outputObj->query;
  $query = $queryObj->sql;
  for ($x = 1;; $x++) {
    $query = preg_replace("/\?/i", "\\$$x", $query, 1, $count);
    if ($count == 0) {
      break;
    }
  }
  
  # Execute the parameterized query
  $rs = pg_query_params($con, $query, $queryObj->params) or die("Cannot execute query: $query\n");
  $rows = array();
  while ($row = pg_fetch_assoc($rs)) {
    $rows[] = $row;
  }
  
  # Encode to JSON and return
  $jsonResult = json_encode($rows);
  return ["json" => $jsonResult];
}

?>
