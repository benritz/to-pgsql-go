# to-pgsql-go
Migration a database to PostgreSQL

## Usage

mssql2pgsql <sqlserver-connection-string> [output-file]

### Flags

- -forceCaseInsensitive: Use citext for case-insensitive text columns (default: true)
- -writeData: Write table data (default: false)
- -dataBatchSize: Batch size for data inserts (default: 100)

## Configuration Schema Notes

Recent change: The `raw_type` field on columns in `config.cue` / YAML configs has been removed. Use `kind` exclusively to specify a column type. Any previous configs using `raw_type` should rename that key to `kind` (values are preserved). Unknown `kind` strings are still passed through as raw types internally.
