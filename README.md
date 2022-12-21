# Conduit Connector MICROSOFT SQL SERVER 

## General

The [SQL SERVER](https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16) connector is one
of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides both, a source and a destination SQL SERVER connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0
- (optional) [mock](https://github.com/golang/mock) 1.6.0


### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests.

## Destination

The SQL Server Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Configuration Options

| Name         | Description                                                                                                                                                                                 | Required | Example                                       |
|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------|
| `connection` | String line  for connection to  SQL SERVER. More information about it [Connection](https://github.com/denisenkom/go-mssqldb#the-connection-string-can-be-specified-in-one-of-three-formats) | **true** | sqlserver://sa:password@0.0.0.0?database=mydb |
| `table`      | The name of a table in the database that the connector should  write to, by default.                                                                                                        | **true** | users                                         |

### Table name

If a record contains a `sqlserver.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a Destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.

### Source

The source connects to the database using the provided connection and starts creating records for each table row
and each detected change.

### Configuration options

| Name             | Description                                                                                                                                                                                                        | Required | Example                                       |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------|
| `connection`     | String line  for connection to  SQL SERVER. More information about it [Connection](https://github.com/denisenkom/go-mssqldb#the-connection-string-can-be-specified-in-one-of-three-formats)                        | **true** | sqlserver://sa:password@0.0.0.0?database=mydb |
| `table`          | The name of a table in the database that the connector should  write to, by default.                                                                                                                               | **true** | users                                         |
| `orderingColumn` | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                                              | **true** | id                                            |
| `primaryKeys`    | Comma separated list of column names that records could use for their `Key` fields. By default connector uses primary keys from table if they are not exist connector will use ordering column.                    | false    | id,name                                       |
| `snapshot`       | Whether or not the plugin will take a snapshot of the entire table before starting cdc mode, by default true.                                                                                                      | false    | false                                         |
| `batchSize`      | Size of rows batch. By default is 1000                                                                                                                                                                             | false    | 100                                           |

### Snapshot
When the connector first starts, snapshot mode is enabled.

First time when the snapshot iterator starts work, it is get max value from `orderingColumn` and saves this value to position.
The snapshot iterator reads all rows, where `orderingColumn` values less or equal maxValue, from the table in batches.


Values in the ordering column must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.
Iterators saves last processed value from `orderingColumn` column to position to field `SnapshotLastProcessedVal`.
If snapshot stops it will parse position from last record and will try gets row where `{{orderingColumn}} > {{position.SnapshotLastProcessedVal}}`


When all records are returned, the connector switches to the CDC iterator.

This behavior is enabled by default, but can be turned off by adding "snapshot":"false" to the Source configuration.

### Change Data Capture (CDC)

This connector implements CDC features for DB2 by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_TRACKING_`. The tracking table has all the
same columns as the target table plus three additional columns:

| name                            | description                                          |
|---------------------------------|------------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.                |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `insert`, `update`, or `delete`.     |
| `CONDUIT_TRACKING_CREATED_DATE` | Date when the event was added to the tracking table. |


Triggers have name pattern `CONDUIT_TRIGGER_{{operation_type}}_{{table}}`.


Queries to retrieve change data from a tracking table are very similar to queries in a Snapshot iterator, but with
`CONDUIT_TRACKING_ID` ordering column.

CDC iterator periodically clears rows which were successfully applied from tracking table.
It collects `CONDUIT_TRACKING_ID` inside the `Ack` method into a batch and clears the tracking table every 5 seconds.

Iterator saves the last `CONDUIT_TRACKING_ID` to the position from the last successfully recorded row.

If connector stops, it will parse position from the last record and will try
to get row where `{{CONDUIT_TRACKING_ID}}` > `{{position.CDCLastID}}`.


### CDC FAQ

#### Is it possible to add/remove/rename column to table?

Yes. You have to stop the pipeline and do the same with conduit tracking table.
For example:
```sql
ALTER TABLE CLIENTS
ADD COLUMN address VARCHAR(18);

ALTER TABLE CONDUIT_TRACKING_CLIENTS
    ADD COLUMN address VARCHAR(18);
```

#### I accidentally removed tracking table.

You have to restart pipeline, tracking table will be recreated by connector.

#### I accidentally removed table.

You have to stop the pipeline, remove the conduit tracking table, and then start the pipeline.

#### Is it possible to change table name?

Yes. Stop the pipeline, change the value of the `table` in the Source configuration,
change the name of the tracking table using a pattern `CONDUIT_TRACKING_{{TABLE}}`
