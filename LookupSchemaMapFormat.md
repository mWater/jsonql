# Format of LookupSchemaMap

LookupSchemaMap is a simple schema map that uses a JSON representation of the schema. It is designed to be compatible with the schema definition of expressions as part of mwater-visualization

See [schema details](https://github.com/mWater/mwater-visualization/blob/master/docs/Schema.md)

This map is an JSON object which has a property `tables` which is an array of tables as defined in the schema. For example:

```
{
  "tables": [
    {
      "id": "programs",
      "name": "Programs",
      "sql": "programs",
      "columns": [
        {
          "id": "gid",
          "type": "id",
          "name": "Number of Programs",
          "sql": "{alias}.gid"
        },
        {
          "id": "programid",
          "type": "text",
          "name": "ID",
          "sql": "{alias}.programid"
        },
        {
          "id": "abbr",
          "type": "text",
          "name": "Abbreviation",
          "sql": "{alias}.poname"
        },
        {
          "id": "name",
          "type": "text",
          "name": "Name",
          "sql": "{alias}.name"
        },
        {
          "id": "ontime",
          "type": "enum",
          "name": "On Time Status",
          "sql": "{alias}.ontime",
          "values": [{ "id": true, "name": "On Time" }, { "id": false, "name": "Delayed" }]
        }
      ]
    }
  ]
}

```


