# to-pgsql-go
Migration a database to PostgreSQL

## Usage

mssql2pgsql <sqlserver-connection-string> [output-file]

### Flags

- -forceCaseInsensitive: Use citext for case-insensitive text columns (default: true)
- -writeData: Write table data (default: false)
- -dataBatchSize: Batch size for data inserts (default: 100)
