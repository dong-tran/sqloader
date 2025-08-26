# SQL Load Tool

This is a command-line tool for load testing a SQL database. It allows you to execute a given SQL query at a specified rate and with a certain level of concurrency, and it measures the latency of the queries.

## Usage

```sh
go run main.go [flags]
```

### Flags

*   `-driver`: The `database/sql` driver to use (e.g., `pgx`, `postgres`, `mysql`). Default: `pgx`.
*   `-dsn`: The database DSN (Data Source Name).
*   `-rps`: The target requests per second. Default: `150`.
*   `-concurrency`: The number of concurrent workers. Default: `32`.
*   `-duration`: The test duration (e.g., `30s`, `5m`). Default: `5m`.
*   `-timeout`: The per-request timeout. Default: `2s`.
*   `-warmup`: The warm-up period (excluded from stats). Default: `2s`.
*   `-sql-file`: The path to the SQL file to execute. Default: `target-sql.txt`.

## Example

```sh
go run main.go -dsn "user=postgres password=secret dbname=testdb sslmode=disable" -rps 200 -concurrency 64 -duration 1m
```

## SQL Query

The SQL query to be executed should be placed in a file named `target-sql.txt` in the same directory as the tool, or you can specify a different file using the `-sql-file` flag.
