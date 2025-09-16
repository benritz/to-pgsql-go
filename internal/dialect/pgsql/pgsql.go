package pgsql

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"benritz/topgsql/internal/dialect"
	"benritz/topgsql/internal/schema"
)

type PgsqlTarget struct {
	conn       *pgx.Conn
	out        *os.File
	textType   string // "", "text" or "citext"
	tableCache map[string]*schema.Table
}

func NewPgsqlTarget(ctx context.Context, targetUrl, textType string) (*PgsqlTarget, error) {
	if textType != "" && textType != "text" && textType != "citext" {
		return nil, fmt.Errorf("Invalid text type: %s (allowed: <empty>, text, citext)", textType)
	}

	target := &PgsqlTarget{
		textType: textType,
	}

	if isConnectionUrl(targetUrl) {
		// database target
		if !strings.HasPrefix(targetUrl, "postgres://") {
			return nil, fmt.Errorf("Target database must be a PostgreSQL connection URL (postgres://username:password@localhost:5432/database_name)")
		}

		conn, err := pgx.Connect(ctx, targetUrl)
		if err != nil {
			return nil, err
		}

		target.conn = conn
	} else {
		// file target
		var out *os.File

		if targetUrl == "" {
			out = os.Stdout
		} else {
			f, err := os.Create(targetUrl)
			if err != nil {
				return nil, err
			}
			out = f
			// out.WriteString("\ufeff") // UTF-8 BOM for Windows
		}

		target.out = out
	}

	return target, nil
}

func (t *PgsqlTarget) Close(ctx context.Context) {
	if t.conn != nil {
		t.conn.Close(ctx)
	}

	if t.out != nil {
		t.out.Close()
	}
}

func (t *PgsqlTarget) GetTables(ctx context.Context) (map[string]*schema.Table, error) {
	if t.tableCache != nil {
		return t.tableCache, nil
	}

	tables, err := getTables(ctx, t.conn)
	if err != nil {
		return nil, err
	}

	t.tableCache = make(map[string]*schema.Table, len(tables))
	for _, table := range tables {
		t.tableCache[translateIdentifier(table.Name)] = table
	}

	return t.tableCache, nil
}

func (t *PgsqlTarget) CreateTables(ctx context.Context, tables []*schema.Table) error {
	if t.out != nil {
		if err := t.writeTablesSchema(tables); err != nil {
			return err
		}
		return nil
	}

	if t.conn != nil {
		if err := t.createTablesSchema(ctx, tables); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) CreateConstraintsAndIndexes(ctx context.Context, tables []*schema.Table) error {
	if t.out != nil {
		fmt.Fprintf(t.out, "/* --------------------- CONSTRAINTS + INDEXES --------------------- */\n\n")
	}

	for _, table := range tables {
		if len(table.Indexes) > 0 {
			if err := t.CreateIndexes(ctx, table.Indexes); err != nil {
				return err
			}
		}
	}

	for _, table := range tables {
		if len(table.ForeignKeyes) > 0 {
			if err := t.CreateForeignKeys(ctx, table.ForeignKeyes); err != nil {
				return err
			}
		}
	}

	if t.out != nil {
		fmt.Fprint(t.out, "\n\n")
	}

	return nil
}

func (t *PgsqlTarget) CreateIndexes(ctx context.Context, indexes []*schema.Index) error {
	if t.out != nil {
		if err := t.writeIndexes(indexes); err != nil {
			return err
		}
		return nil
	}

	if t.conn != nil {
		if err := t.createIndexes(ctx, indexes); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) CreateForeignKeys(ctx context.Context, keys []*schema.ForeignKey) error {
	if t.out != nil {
		if err := t.writeForeignKeys(keys); err != nil {
			return err
		}
	}

	if t.conn != nil {
		if err := t.createForeignKeys(ctx, keys); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) CopyTables(ctx context.Context, tables []*schema.Table, reader dialect.TableDataReader) error {
	if t.out != nil {
		for _, table := range tables {
			if err := t.writeTableData(ctx, table, reader); err != nil {
				return err
			}
		}

		t.writeSeqReset()
	}

	if t.conn != nil {
		if err := setReplicationOn(ctx, t.conn); err != nil {
			return err
		}
		defer setReplicationOff(ctx, t.conn)

		for _, table := range tables {
			if err := t.copyTableData(ctx, table, reader); err != nil {
				return fmt.Errorf("Copy data failed for %s: %v", table.Name, err)
			}
		}

		if err := t.execSeqReset(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (t *PgsqlTarget) CreateViews(ctx context.Context, views []*schema.View) error {
	if t.out != nil {
		if err := t.writeViews(views); err != nil {
			return err
		}
		return nil
	}

	if t.conn != nil {
		// if err := t.createViews(ctx, views); err != nil {
		// 	return err
		// }
		return nil
	}

	return nil
}

func (t *PgsqlTarget) RunScript(ctx context.Context, script string) error {
	if t.out != nil {
		fmt.Fprintf(t.out, "/* --------------------- SCRIPT --------------------- */\n\n%s\n\n", script)
		return nil
	}

	if t.conn != nil {
		if _, err := t.conn.Exec(ctx, script); err != nil {
			return fmt.Errorf("Failed to execute script: %v", err)
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) writeTableData(ctx context.Context, table *schema.Table, reader dialect.TableDataReader) error {
	if err := reader.Open(ctx, table.Name, table.Columns); err != nil {
		return err
	}

	var copyRows [][]any
	CopyBatchSize := 1000

	targetTableName := translateIdentifier(table.Name)

	written := false

	copy := func() error {
		if len(copyRows) > 0 {
			if !written {
				fmt.Fprintf(t.out, "-- %s\n", table.Name)
				written = true
			}

			fmt.Fprintf(t.out, "insert into %s values", targetTableName)

			for n, row := range copyRows {
				valStrs := make([]string, len(row))
				for i, val := range row {
					valStrs[i] = convertValueToString(val, table.Columns[i].DataType)
				}

				if n > 0 {
					fmt.Fprintf(t.out, ",")
				}

				fmt.Fprintf(t.out, "\n\t(%s)", strings.Join(valStrs, ", "))
			}

			fmt.Fprintf(t.out, ";\n\n")
		}

		return nil
	}

	for {
		row, err := reader.ReadRow()
		if err != nil {
			reader.Close(ctx)
			return err
		}

		if len(row) == 0 {
			break
		}

		copyRow := make([]any, len(table.Columns))

		for i, data := range row {
			copyRow[i] = ConvertValue(data, table.Columns[i].DataType)
		}

		copyRows = append(copyRows, copyRow)

		if len(copyRows) >= CopyBatchSize {
			if err := copy(); err != nil {
				reader.Close(ctx)
				return err
			}

			copyRows = copyRows[:0]
		}
	}

	if err := reader.Close(ctx); err != nil {
		return err
	}

	if err := copy(); err != nil {
		return err
	}

	return nil
}

func (t *PgsqlTarget) writeSeqReset() {
	fmt.Fprintf(t.out, "")

	fmt.Fprintf(
		t.out,
		"/* --------------------- SEQUENCE RESET --------------------- */\n\n%s\n\n-- set any sequence to the maximum value of the sequence's field\n%s\n\n",
		CreateSeqResetFnStatement(),
		ExecSeqResetFnStatement(),
	)
}

func (t *PgsqlTarget) copyTableData(ctx context.Context, table *schema.Table, reader dialect.TableDataReader) error {
	tablesMap, err := t.GetTables(ctx)
	if err != nil {
		return err
	}

	targetTableName := translateIdentifier(table.Name)

	targetTable, ok := tablesMap[targetTableName]
	if !ok {
		sql := CreateTableStatement(table, t.textType)
		_, err := t.conn.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("Table %s not found in target database. Failed to create table: %v", targetTableName, err)
		}

		targetTable = table
	}

	cols := schema.UpdateableColumns(targetTable)

	colNames := make([]string, len(cols))
	for n, col := range cols {
		colNames[n] = translateIdentifier(col.Name)
	}

	if err := reader.Open(ctx, table.Name, cols); err != nil {
		return err
	}

	sql := fmt.Sprintf("truncate table %s cascade;", escapeIdentifier(targetTableName))
	if _, err := t.conn.Exec(ctx, sql); err != nil {
		return err
	}

	var copyRows [][]any
	CopyBatchSize := 1000

	copy := func() error {
		if len(copyRows) > 0 {
			count, err := t.conn.CopyFrom(ctx, pgx.Identifier{targetTableName}, colNames, pgx.CopyFromRows(copyRows))
			if err != nil {
				return err
			}

			fmt.Printf("Copied %d rows into %s\n", count, table.Name)
		}

		return nil
	}

	for {
		row, err := reader.ReadRow()
		if err != nil {
			reader.Close(ctx)
			return err
		}

		if len(row) == 0 {
			break
		}

		copyRow := make([]any, len(cols))

		for i, data := range row {
			copyRow[i] = ConvertValue(data, cols[i].DataType)
		}

		copyRows = append(copyRows, copyRow)

		if len(copyRows) >= CopyBatchSize {
			if err := copy(); err != nil {
				reader.Close(ctx)
				return err
			}

			copyRows = copyRows[:0]
		}
	}

	if err := reader.Close(ctx); err != nil {
		return err
	}

	if err := copy(); err != nil {
		return err
	}

	return nil
}

func (t *PgsqlTarget) execSeqReset(ctx context.Context) error {
	if _, err := t.conn.Exec(ctx, CreateSeqResetFnStatement()); err != nil {
		return err
	}

	if _, err := t.conn.Exec(ctx, ExecSeqResetFnStatement()); err != nil {
		return err
	}

	return nil
}

func (t *PgsqlTarget) writeTablesSchema(tables []*schema.Table) error {
	fmt.Fprintf(t.out, "/* --------------------- TABLES --------------------- */\n\n")

	if t.textType == "citext" {
		fmt.Fprintf(t.out, "create extension if not exists citext;\n\n")
	}

	for _, table := range tables {
		if err := t.writeTableSchema(table); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeTableSchema(table *schema.Table) error {
	drop := DropTableStatement(table)
	create := CreateTableStatement(table, t.textType)

	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n%s\n\n%s\n\n",
		table.Name,
		drop,
		create,
	)

	return nil
}

func (t *PgsqlTarget) createTablesSchema(ctx context.Context, tables []*schema.Table) error {
	if _, err := t.conn.Exec(ctx, "create extension if not exists citext;\n\n"); err != nil {
		return err
	}

	for _, table := range tables {
		if err := t.createTableSchema(ctx, table); err != nil {
			return err
		}
	}

	return nil
}

func (t *PgsqlTarget) createTableSchema(ctx context.Context, table *schema.Table) error {
	sql := CreateTableStatement(table, t.textType)
	_, err := t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("Failed to create table %s: %v", table.Name, err)
	}
	return nil
}

func (t *PgsqlTarget) writeIndexes(indexes []*schema.Index) error {
	for _, index := range indexes {
		if err := t.writeIndex(index); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeIndex(index *schema.Index) error {
	create := CreateIndexStatement(index)
	fmt.Fprint(t.out, create)

	return nil
}

func (t *PgsqlTarget) createIndexes(ctx context.Context, indexes []*schema.Index) error {
	for _, index := range indexes {
		if err := t.createIndex(ctx, index); err != nil {
			return err
		}
	}

	return nil
}

func (t *PgsqlTarget) createIndex(ctx context.Context, index *schema.Index) error {
	sql := CreateIndexStatement(index)
	_, err := t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("Failed to create index %v: %v", index, err)
	}

	return nil
}

func (t *PgsqlTarget) writeForeignKeys(keys []*schema.ForeignKey) error {
	for _, key := range keys {
		if err := t.writeForeignKey(key); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeForeignKey(key *schema.ForeignKey) error {
	create := CreateForeignKeyStatement(key)
	fmt.Fprint(t.out, create)

	return nil
}

func (t *PgsqlTarget) createForeignKeys(ctx context.Context, keys []*schema.ForeignKey) error {
	for _, key := range keys {
		if err := t.createForeignKey(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

func (t *PgsqlTarget) createForeignKey(ctx context.Context, key *schema.ForeignKey) error {
	sql := CreateForeignKeyStatement(key)
	_, err := t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("Failed to create foreign key %v: %v", key, err)
	}

	return nil
}

func (t *PgsqlTarget) writeViews(views []*schema.View) error {
	fmt.Fprintf(t.out, "/* --------------------- VIEWS --------------------- */\n\n")

	for _, view := range views {
		if err := t.writeView(view); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeView(view *schema.View) error {
	drop := DropViewStatement(view)
	create := CreateViewStatement(view)

	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n%s\n\n%s\n\n",
		view.Name,
		drop,
		create,
	)

	return nil
}

func translateIdentifier(identifier string) string {
	return strings.ToLower(identifier)
}

func escapeIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

func isConnectionUrl(url string) bool {
	return !strings.HasPrefix(url, "file://") && strings.Contains(url, "://")
}

func setReplicationOn(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "set session_replication_role = 'replica';")
	return err
}

func setReplicationOff(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "set session_replication_role = 'origin';")
	return err
}

func getTables(ctx context.Context, conn *pgx.Conn) ([]*schema.Table, error) {
	rows, err := conn.Query(ctx, `
SELECT
    c.relname AS table_name,
    a.attnum  AS column_id,
    a.attname AS column_name,
    a.atttypmod,
    t.typname AS base_type,
    NOT a.attnotnull AS is_nullable,
    (a.attidentity <> '') AS is_identity,
    (a.attgenerated <> '') AS is_generated,
    pg_get_expr(ad.adbin, ad.adrelid) AS default_expr
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY c.relname ASC, c.oid ASC, a.attnum ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*schema.Table{}
	var lastTable string
	var columns []*schema.Column

	for rows.Next() {
		var (
			tableName, columnName, baseType string
			attTypMod                       int
			columnID                        int
			isNullable, isIdentity          bool
			isGenerated                     bool
			defaultValue                    *string
		)

		if err := rows.Scan(
			&tableName,
			&columnID,
			&columnName,
			&attTypMod,
			&baseType,
			&isNullable,
			&isIdentity,
			&isGenerated,
			&defaultValue,
		); err != nil {
			return nil, err
		}

		if columnID == 1 {
			if lastTable != "" {
				tables = append(tables, &schema.Table{Name: lastTable, Columns: columns})
			}
			lastTable = tableName
			columns = []*schema.Column{}
		}

		maxLength := 0
		precision := 0
		scale := 0

		switch baseType {
		case "varchar", "bpchar", "char":
			if attTypMod > 4 {
				maxLength = attTypMod - 4
			}
		case "numeric", "decimal":
			if attTypMod > 4 {
				typmod := attTypMod - 4
				precision = (typmod >> 16) & 0xffff
				scale = typmod & 0xffff
			}
		}

		isAutoInc := isIdentity

		if defaultValue == nil {
			defaultValue = new(string)
		} else if !isAutoInc &&
			strings.HasPrefix(strings.ToLower(strings.TrimSpace(*defaultValue)), "nextval(") {
			isAutoInc = true
		}

		dt := toDataType(baseType, maxLength, precision, scale, isAutoInc)

		columns = append(columns, &schema.Column{
			ColumnID:   columnID,
			Name:       columnName,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
			IsComputed: isGenerated,
			IsAutoInc:  isAutoInc,
			Default:    *defaultValue,
			DataType:   dt,
		})
	}

	if lastTable != "" {
		tables = append(tables, &schema.Table{Name: lastTable, Columns: columns})
	}

	return tables, nil
}

func toDataType(baseType string, maxLength, precision, scale int, isAutoInc bool) schema.DataType {
	raw := strings.ToLower(baseType)
	dt := schema.DataType{Kind: schema.KindUnknown, Raw: raw}
	switch raw {
	case "bool", "boolean":
		dt.Kind = schema.KindBool
	case "smallint", "int2":
		dt.Kind = schema.KindInt16
	case "integer", "int", "int4":
		if isAutoInc {
			dt.Kind = schema.KindSerialInt32
		} else {
			dt.Kind = schema.KindInt32
		}
	case "bigint", "int8":
		if isAutoInc {
			dt.Kind = schema.KindSerialInt64
		} else {
			dt.Kind = schema.KindInt64
		}
	case "real", "float4":
		dt.Kind = schema.KindFloat32
	case "double precision", "float8":
		dt.Kind = schema.KindFloat64
	case "numeric", "decimal":
		dt.Kind = schema.KindNumeric
		dt.Precision = precision
		dt.Scale = scale
	case "uuid":
		dt.Kind = schema.KindUUID
	case "varchar", "character varying":
		if maxLength > 0 {
			dt.Kind = schema.KindVarChar
			dt.Length = maxLength
		} else {
			dt.Kind = schema.KindText
		}
	case "text":
		dt.Kind = schema.KindText
	case "bytea":
		dt.Kind = schema.KindBinary
	case "date":
		dt.Kind = schema.KindDate
	case "time", "timetz":
		dt.Kind = schema.KindTime
	case "timestamp":
		dt.Kind = schema.KindTimestamp
	case "timestamptz", "timestamp with time zone":
		dt.Kind = schema.KindTimestamp
		dt.Timezone = true
	case "json", "jsonb":
		dt.Kind = schema.KindJson
	}
	return dt
}

func fromDatatype(dt schema.DataType, textType string) string {
	switch dt.Kind {
	case schema.KindSerialInt32:
		return "serial"
	case schema.KindSerialInt64:
		return "bigserial"
	case schema.KindInt16:
		return "smallint"
	case schema.KindInt32:
		return "int"
	case schema.KindInt64:
		return "bigint"
	case schema.KindBool:
		return "boolean"
	case schema.KindUUID:
		return "uuid"
	case schema.KindFloat32:
		return "real"
	case schema.KindFloat64:
		return "double precision"
	case schema.KindNumeric:
		if dt.Precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", dt.Precision, dt.Scale)
		}
		return "numeric"
	case schema.KindMoney:
		return "numeric(19,4)"
	case schema.KindVarChar:
		if textType == "text" {
			return "text"
		}
		if textType == "citext" {
			return "citext"
		}
		if dt.Length <= 0 {
			return "text"
		}
		return fmt.Sprintf("varchar(%d)", dt.Length)
	case schema.KindText:
		if textType == "citext" {
			return "citext"
		}
		return "text"
	case schema.KindBinary:
		return "bytea"
	case schema.KindDate:
		return "date"
	case schema.KindTime:
		return "time"
	case schema.KindTimestamp:
		if dt.Timezone {
			return "timestamptz"
		}
		return "timestamp"
	case schema.KindJson:
		return "jsonb"
	case schema.KindUnknown:
		fallthrough
	default:
		return "text"
	}
}

func DropTableStatement(table *schema.Table) string {
	return fmt.Sprintf("drop table if exists %s;", table.Name)
}

func CreateTableStatement(table *schema.Table, textType string) string {
	columnDefs := ""

	for i, column := range table.Columns {
		if i > 0 {
			columnDefs += ",\n"
		}

		columnDefs += "\t" + column.Name + " "
		columnDefs += fromDatatype(column.DataType, textType)

		if column.Default != "" && !column.IsAutoInc {
			columnDefs += " default " + translateMarkers(column.Default)
		}
		if !column.IsNullable {
			columnDefs += " not"
		}
		columnDefs += " null"
	}

	sql := fmt.Sprintf(
		"create table %s\n(\n%s\n);",
		table.Name,
		columnDefs,
	)

	return sql
}

func DropViewStatement(view *schema.View) string {
	return fmt.Sprintf("drop view if exists %s;", view.Name)
}

func CreateViewStatement(view *schema.View) string {
	sql := translateMarkers(view.Definition)

	return sql
}

func translateMarkers(s string) string {
	s = strings.ReplaceAll(s, "{{Now.Local}}", "now()")
	s = strings.ReplaceAll(s, "{{Now.UTC}}", "now() at time zone 'utc'")
	s = strings.ReplaceAll(s, "{{True}}", "true")
	s = strings.ReplaceAll(s, "{{False}}", "false")
	return s
}

func CreateIndexStatement(index *schema.Index) string {
	cols := strings.Join(index.Columns, ", ")
	include := ""
	if len(index.IncludeColumns) > 0 {
		include = " include (" + strings.Join(index.IncludeColumns, ", ") + ")"
	}
	filter := ""
	if index.Filter != "" {
		filter = " where " + translateMarkers(index.Filter)
	}
	switch index.IndexType {
	case schema.IndexTypePrimaryKey:
		return fmt.Sprintf("alter table %s add constraint %s primary key (%s);\n", index.Table, index.Name, cols)
	case schema.IndexTypeUniqueConstraint:
		return fmt.Sprintf("alter table %s add constraint %s unique (%s);\n", index.Table, index.Name, cols)
	case schema.IndexTypeUnique:
		return fmt.Sprintf("create unique index %s on %s (%s)%s%s;\n", index.Name, index.Table, cols, include, filter)
	case schema.IndexTypeNonUnique:
		fallthrough
	default:
		return fmt.Sprintf("create index %s on %s (%s)%s%s;\n", index.Name, index.Table, cols, include, filter)
	}
}

func CreateForeignKeyStatement(key *schema.ForeignKey) string {
	parentCols := strings.Join(key.Columns, ", ")
	refCols := strings.Join(key.ReferencedColumns, ", ")
	return fmt.Sprintf(
		"alter table %s add constraint %s foreign key (%s) references %s (%s);\n",
		key.Table,
		key.Name,
		parentCols,
		key.ReferencedTable,
		refCols,
	)
}

func CreateSeqResetFnStatement() string {
	return `create or replace function seq_field_max_value(seq_oid oid) returns bigint
volatile strict language plpgsql as $$ 
declare
    target_table regclass;
    target_column name;
    max_value bigint;
begin
    -- Find the table and column that the sequence is associated with
    -- using a much simpler and more direct approach.
    select distinct 
        d.refobjid::regclass,
        a.attname
    into 
        target_table,
        target_column
    from pg_depend d
    join pg_class c on d.objid = c.oid and c.relkind = 'S'
    join pg_attribute a on d.refobjid = a.attrelid and d.refobjsubid = a.attnum
    where c.oid = seq_oid
      and d.deptype = 'a'; -- 'a' denotes an automatic dependency from a sequence to a column

    -- Check if a dependency was found
    if target_table is null or target_column is null then
        return null;
    end if;

    -- Execute a single dynamic query to find the maximum value in the column
    execute 'select max(' || quote_ident(target_column) || ') from ' || target_table
    into max_value;

    return max_value;

end;
$$;`
}

func ExecSeqResetFnStatement() string {
	return "select relname, setval(oid, seq_field_max_value(oid)) from pg_class where relkind = 'S';"
}

func convertValueToString(val any, dt schema.DataType) string {
	val = ConvertValue(val, dt)

	if val == nil {
		return "null"
	}

	switch dt.Kind {
	case schema.KindVarChar, schema.KindText, schema.KindUUID:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''"))
	case schema.KindDate:
		ts := val.(time.Time)
		return fmt.Sprintf("'%s'", ts.UTC().Format("2006-01-02"))
	case schema.KindTime:
		ts := val.(time.Time)
		return fmt.Sprintf("'%s'", ts.UTC().Format("15:04:05.999999Z07:00"))
	case schema.KindTimestamp:
		ts := val.(time.Time)
		return fmt.Sprintf("'%s'", ts.UTC().Format("2006-01-02 15:04:05.999999Z07:00"))
	default:
		return fmt.Sprintf("%v", val)
	}
}

func ConvertValue(val any, dt schema.DataType) any {
	switch v := val.(type) {
	case nil:
		return nil
	case []byte:
		switch dt.Kind {
		case schema.KindNumeric, schema.KindMoney:
			return strings.TrimSpace(string(v))
		case schema.KindUUID:
			if len(v) == 16 {
				return uuidFromBytes(v)
			} else if len(v) == 0 {
				return nil
			}
			return strings.ToLower(strings.TrimSpace(string(v)))
		case schema.KindBool:
			if len(v) > 0 && v[0] != 0 {
				return true
			}
			return false
		default:
			return v
		}
	case string:
		switch dt.Kind {
		case schema.KindNumeric, schema.KindMoney:
			return strings.TrimSpace(v)
		case schema.KindUUID:
			return strings.ToLower(strings.TrimSpace(v))
		default:
			return dialect.StripNull(v)
		}
	case time.Time:
		return v.UTC()
	default:
		return v
	}
}

func uuidFromBytes(b []byte) string {
	if len(b) != 16 {
		return ""
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
}
