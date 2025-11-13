# to-pgsql-go - Migrate a database to PostgreSQL

Migrate a MS SQL Server database to PostgreSQL using a script or directly. 

## Build

- Host: `make build` outputs to `dist/to-pgsql`
- Linux Arm64 + Amd64: `make build-linux` outputs `dist/to-pgsql-linux-amd64` and `dist/to-pgsql-linux-arm64`
- Windows Arm64 + Amd64: `make build-windows` outputs `dist/to-pgsql-windows-amd64.exe` and `dist/to-pgsql-windows-arm64.exe`
- Both Linux + Wndows: `make build-all` 

## Usage

### CLI Parameters

- `-config` - Configuration file path
- `-source` - Source database connection URL
- `-target` - Target file or database connection URL
- `-textType` - How to convert the text column schema. Either text, citext or varchar (default: citext)
- `-incTables` - Include tables schema. Either none (default), create or recreate
- `-incData` - Include table data. Either none (default), insert, overwrite or merge
- `-incFunctions` - Include functions (default: false)
- `-incProcedures` - Include procedures (default: false)
- `-incTriggers` - Include triggers (default: false)
- `-incViews` - Include views (default: false)
- `-dataBatchSize` - Batch size for data inserts 

