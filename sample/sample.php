<?php 
$rootDir = dirname(dirname(__FILE__));
include "$rootDir/lib/query.php";

#CONNECT TO DATABASE
$host = "localhost"; 
$user = "user12"; 
$pass = "34klq*"; 
$db = "testdb"; 
$con = pg_connect("host=$host dbname=$db user=$user password=$pass")
    or die ("Could not connect to server\n"); 


#INSERT TEST DATA

$query = "DROP TABLE IF EXISTS cars"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "CREATE TABLE cars(id INTEGER PRIMARY KEY, name VARCHAR(25), price INT)";  
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(1,'Audi',52642)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(2,'Mercedes',57127)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(3,'Skoda',9000)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(4,'Volvo',29000)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(5,'Bentley',350000)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(6,'Citroen',21000)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(7,'Hummer',41400)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 

$query = "INSERT INTO cars VALUES(8,'Volkswagen',21606)"; 
pg_query($con, $query) or die("Cannot execute query: $query\n"); 


#DEFINE JSONQL QUERY                      
$jqlQuery = ["type" => "query",
             "selects" => [["type" => "select",
                            "expr" => ["type" => "field",
                                       "tableAlias" => "supercars",
                                       "column" => "name"],
                            "alias" => "supername"]],
             "from" => ["type" => "table",
                        "table" => "cars",
                        "alias" => "supercars"],
             "where" => ["type" => "op",
                         "op" => ">",
                         "exprs" => [["type" => "field",
                                      "tableAlias" => "supercars",
                                      "column" => "price"],
                                     ["type" => "literal",
                                      "value" => 30000]]]];

#DEFINE TABLE MAP FILENAME
$tableMapFile = "$rootDir/sample/sampleTableMap.json";

#GET JSON-FORMATTED QUERY RESULTS BASED ON INPUT
$result = query($jqlQuery, $tableMapFile, $con);

#DISPLAY
if (array_key_exists("error", $result)) {
  $error = $result["error"];
  echo "Error: $error";
} elseif (array_key_exists("json", $result)) {
  $json = $result["json"];
  echo "JSON: $json";
} else {
  echo "Error: Invalid PHP return value";
}

pg_close($con); 
?>
