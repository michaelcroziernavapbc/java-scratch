## Michael's Tiny Java Experiments

### db2downloader

A crude JDBC client executes a SQL query and streams the results to a parquet file, with configurable row group chunkery.

Setup:
- create or link to a directory with the DB2 JDBC driver, at db2downloader/jdbc_drivers
- create a JSON file with the job details, example below
- Run! gradle db2downloader:run --args="/path/to/your.json"

example.json:
```json
{
  "url": "jdbc:db2://hostname:446/databasename:useJDBC4ColumnNameAndLabelSemantics=NO;",
  "username": "MICAHEL CORZIER",
  "password": "SUPERSECRET",
  "query": "select * from skema.neato_table",
  "batchSize": 50000,
  "outputPath": "neato_table.parquet"
}
```