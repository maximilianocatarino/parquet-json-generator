# parquet-json-generator
Generate Apache Parquet files based on JSON schema.

# Schema Description
```
tableName: Parquet file name
Schema: column schema collection
Schema.columnName: name of the column
Schema.type: type of the column. [Check the valid types for DuckDB here.](https://duckdb.org/docs/sql/data_types/overview). 
Schema.key: define column as primary key
Schema..nullable: allow/deny null value for the column. Default, allow null values.
```

# Schema Example
```
{
    "tableName": "cloud_provider",
    "schema": [
        {"columnName": "id", "type": "uinteger", "key": true, "nullable": false},
        {"columnName": "description", "type": "varchar", "nullable": false},
        {"columnName": "created_at", "type": "timestamp", "nullable": false}
    ],
    "rows": [
        {"id": 1, "description": "Amazon Web Services", "created_at": "to_timestamp(1695406053139)"},
        {"id": 2, "description": "Google Cloud", "created_at": "to_timestamp(1695406053139)"},
        {"id": 3, "description": "Microsoft Azure", "created_at": "to_timestamp(1695406053139)"}
    ]
}
```
