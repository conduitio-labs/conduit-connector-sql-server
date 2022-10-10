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

The DB2 Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Configuration Options

| Name         | Description                                                                                                                                                                                 | Required | Example                                       |
|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------|
| `connection` | String line  for connection to  SQL SERVER. More information about it [Connection](https://github.com/denisenkom/go-mssqldb#the-connection-string-can-be-specified-in-one-of-three-formats) | **true** | sqlserver://sa:password@0.0.0.0?database=mydb |
| `table`      | The name of a table in the database that the connector should  write to, by default.                                                                                                        | **true** | users                                         |

### Table name

If a record contains a `sqlserver.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a Destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.
