# to-pgsql-go - Migrate a database to PostgreSQL

Migrate a MS SQL Server database to PostgreSQL using a script or directly. 

## Build

- Host: `make build` outputs to `dist/to-pgsql`
- Linux Arm64 + Amd64: `make build-linux` outputs `dist/to-pgsql-linux-amd64` and `dist/to-pgsql-linux-arm64`
- Windows Arm64 + Amd64: `make build-windows` outputs `dist/to-pgsql-windows-amd64.exe` and `dist/to-pgsql-windows-arm64.exe`
- Both Linux + Wndows: `make build-all` 

## Usage

### CLI Parameters

Use a configuration file for repeated migrations. 
CLI parameters override configuration file parameters.

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
- `-verifyData` - Verify data integrity 

### Configuration File Parameters

The configuration file supports the following structure:

```yaml
source:
  url: "Source database connection URL formatted as sqlserver://${SOURCE_USER}:${SOURCE_PWD}@${SOURCE_HOST}?database=${SOURCE_DB}"

target:
  url: "Target database connection URL formatted as postgres://${TARGET_USER}:${TARGET_PWD}@${TARGET_HOST}/${TARGET_DB}"
  text_type: "How to convert text columns (text, citext, varchar)"
  data_batch_size: "Batch size for data inserts"
  constraints_after_data: "Defer creating constraints & indexes until after data copy (true/false)"
  verify_data: "Verify data integrity (true/false)"

include:
  tables: "Include tables schema (none, create, recreate)"
  data: "Include table data (none, insert, overwrite, merge)"
  functions: "Include functions (true/false)"
  triggers: "Include triggers (true/false)"
  procedures: "Include procedures (true/false)"
  views: "Include views (true/false)"

schema:
  tables:
    - name: "Table name"
      strategy: "Table strategy (replace, merge)"
      columns:
        - name: "Column name"
          kind: "Column type"
          length: "Column length"
          precision: "Column precision"
          scale: "Column scale"
          timezone: "Timezone support (true/false)"
          nullable: "Nullable (true/false)"
          auto_increment: "Auto increment (true/false)"
          computed: "Computed column (true/false)"
          default: "Default value"
      indexes:
        - name: "Index name"
          type: "Index type (non_unique, primary_key, unique_constraint, unique)"
          columns: ["Column names"]
          include_columns: ["Additional columns to include"]
          filter: "Index filter condition"
      foreign_keys:
        - name: "Foreign key name"
          columns: ["Local columns"]
          referenced_table: "Referenced table name"
          referenced_columns: ["Referenced columns"]

scripts: ["Script file paths"]
scripts_path: "Base path for script files"
scripts_expand_env: "Expand environment variables in scripts (true/false)"
```

