package pgsql

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	if t.conn == nil || t.tableCache != nil {
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

	indexes, err := readIndexes(ctx, t.conn)
	if err != nil {
		return nil, err
	}
	for _, idx := range indexes {
		if tbl, ok := t.tableCache[translateIdentifier(idx.Table)]; ok {
			tbl.Indexes = append(tbl.Indexes, idx)
		}
	}

	keys, err := readForeignKeys(ctx, t.conn)
	if err != nil {
		return nil, err
	}
	for _, fk := range keys {
		if tbl, ok := t.tableCache[translateIdentifier(fk.Table)]; ok {
			tbl.ForeignKeyes = append(tbl.ForeignKeyes, fk)
		}
	}

	return t.tableCache, nil
}

func (t *PgsqlTarget) CreateTables(
	ctx context.Context,
	tables []*schema.Table,
	recreate bool,
) error {
	if t.out != nil {
		if err := t.writeTablesSchema(tables, recreate); err != nil {
			return err
		}
		return nil
	}

	if t.conn != nil {
		if err := t.createTablesSchema(ctx, tables, recreate); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) CreateConstraintsAndIndexes(
	ctx context.Context,
	tables []*schema.Table,
	recreate bool,
) error {
	if t.out != nil {
		fmt.Fprintf(t.out, "/* --------------------- CONSTRAINTS + INDEXES --------------------- */\n\n")
	}

	var err error

	var tablesMap map[string]*schema.Table
	if !recreate {
		tablesMap, err = t.GetTables(ctx)
		if err != nil {
			return err
		}
	}

	for _, table := range tables {
		indexes := table.Indexes

		if !recreate {
			targetTable, ok := tablesMap[translateIdentifier(table.Name)]
			if ok {
				indexes = schema.MissingIndexes(table, targetTable)
			}
		}

		if len(indexes) > 0 {
			if err = t.CreateIndexes(ctx, indexes); err != nil {
				return err
			}
		}
	}

	for _, table := range tables {
		keys := table.ForeignKeyes

		if !recreate {
			targetTable, ok := tablesMap[translateIdentifier(table.Name)]
			if ok {
				keys = schema.MissingForeignKeys(table, targetTable)
			}
		}

		if len(keys) > 0 {
			if err = t.CreateForeignKeys(ctx, keys); err != nil {
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

func (t *PgsqlTarget) CopyTables(
	ctx context.Context,
	tables []*schema.Table,
	reader dialect.TableDataReader,
	action dialect.CopyAction,
) error {
	if t.out != nil {
		fmt.Fprint(t.out, "/* -- Table data -- */\n\n")

		if action != dialect.CopyInsert {
			fmt.Fprint(t.out, "set session_replication_role = 'replica';\n\n")

			if action == dialect.CopyOverwrite {
				for _, table := range tables {
					if err := t.truncateTableData(ctx, table); err != nil {
						return err
					}
				}
			}
		}

		for _, table := range tables {
			if err := t.writeTableData(ctx, table, reader, action); err != nil {
				return err
			}
		}

		if action != dialect.CopyInsert {
			fmt.Fprint(t.out, "set session_replication_role = 'origin';\n\n")
		}

		t.writeSeqReset()
	}

	if t.conn != nil {
		// txErr is used by useReplication() defer func to commit or rollback the transaction
		var txErr error

		// use replication to prevent triggers/foreign key issues
		// when connection pooling is used a transaction must be used to ensure the same connection
		// is used (the pool mode should be transaction)
		// returns a function to commit/rollback the tranaction based on the txErr value and reset the replication setting
		useReplication := func(ctx context.Context) (*func(), error) {
			tx, err := t.conn.Begin(ctx)
			if err != nil {
				return nil, err
			}

			fmt.Println("trans begin")

			if err := setReplicationOn(ctx, t.conn); err != nil {
				tx.Rollback(ctx)
				fmt.Println("repl on failed, trans rollback")
				return nil, err
			}

			completeFn := func() {
				setReplicationOff(ctx, t.conn)

				if txErr == nil {
					tx.Commit(ctx)
					fmt.Println("trans commit")
				} else {
					tx.Rollback(ctx)
					fmt.Println("trans rollback")
				}
			}

			return &completeFn, nil
		}

		if action != dialect.CopyInsert {
			// use replication to prevent triggers/foreign key issues
			// data can be populated before target tables are populated
			// data can be truncated and re-inserted
			completeFn, err := useReplication(ctx)
			if err != nil {
				return err
			}
			defer (*completeFn)()

			if action == dialect.CopyOverwrite {
				for _, table := range tables {
					if txErr = t.truncateTableData(ctx, table); txErr != nil {
						return txErr
					}
				}
			}
		}

		for _, table := range tables {
			if txErr = t.copyTableData(ctx, table, reader, action); txErr != nil {
				return fmt.Errorf("copy data failed for %s: %v", table.Name, txErr)
			}
		}

		if txErr = t.execSeqReset(ctx); txErr != nil {
			return txErr
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
		return fmt.Errorf("create views is unsupported")
	}

	return nil
}

func (t *PgsqlTarget) CreateScripts(ctx context.Context, scripts []string, expandEnv bool) error {
	if t.out != nil {
		fmt.Fprintf(t.out, "/* --------------------- SCRIPTS --------------------- */\n\n")
	}

	for _, path := range scripts {
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		contents := string(content)
		if expandEnv {
			contents = dialect.ExpandEnvMarkers(contents)
		}

		name := filepath.Base(path)

		if err := t.createScript(ctx, name, contents); err != nil {
			return fmt.Errorf("failed to create script: %w", err)
		}
	}

	return nil
}

func (t *PgsqlTarget) createScript(ctx context.Context, name string, contents string) error {
	if t.out != nil {
		fmt.Fprintf(t.out, "/* -- %s -- */\n\n%s\n\n", name, contents)
		return nil
	}

	if t.conn != nil {
		if _, err := t.conn.Exec(ctx, contents); err != nil {
			return fmt.Errorf("Failed to execute script: %s %v", name, err)
		}
		return nil
	}

	return nil
}

func (t *PgsqlTarget) CreateTriggers(ctx context.Context, triggers []*schema.Trigger) error {
	if t.out != nil {
		if err := t.writeTriggers(triggers); err != nil {
			return err
		}
		return nil
	}
	if t.conn != nil {
		return fmt.Errorf("create triggers is unsupported")
	}
	return nil
}

func (t *PgsqlTarget) CreateProcedures(ctx context.Context, procedures []*schema.Procedure) error {
	if t.out != nil {
		if err := t.writeProcedures(procedures); err != nil {
			return err
		}
		return nil
	}
	if t.conn != nil {
		return fmt.Errorf("create procedures is unsupported")
	}
	return nil
}

func (t *PgsqlTarget) CreateFunctions(ctx context.Context, functions []*schema.Function) error {
	if t.out != nil {
		if err := t.writeFunctions(functions); err != nil {
			return err
		}
		return nil
	}
	if t.conn != nil {
		return fmt.Errorf("create functions is unsupported")
	}
	return nil
}

func (t *PgsqlTarget) truncateTableData(
	ctx context.Context,
	table *schema.Table,
) error {

	targetTableName := escapeIdentifier(translateIdentifier(table.Name))
	sql := fmt.Sprintf("truncate table %s;", targetTableName)

	if t.out != nil {
		fmt.Fprint(t.out, sql)
		return nil
	}

	tag, err := t.conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	fmt.Printf("%s - copy data - truncated %d row/s\n", table.Name, tag.RowsAffected())

	return nil
}

func (t *PgsqlTarget) writeTableData(
	ctx context.Context,
	table *schema.Table,
	reader dialect.TableDataReader,
	action dialect.CopyAction,
) error {
	if err := reader.Open(ctx, table.Name, table.Columns); err != nil {
		return err
	}

	var rows [][]any
	PrintBatchSize := 1000

	targetTableName := escapeIdentifier(translateIdentifier(table.Name))

	written := false

	printRows := func() error {
		if len(rows) > 0 {
			if !written {
				fmt.Fprintf(t.out, "-- %s\n", table.Name)
				written = true
			}

			fmt.Fprintf(t.out, "insert into %s values", targetTableName)

			for n, row := range rows {
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

		rowConverted := make([]any, len(table.Columns))

		for i, v := range row {
			rowConverted[i] = ConvertValue(v, table.Columns[i].DataType)
		}

		rows = append(rows, rowConverted)

		if len(rows) >= PrintBatchSize {
			if err := printRows(); err != nil {
				reader.Close(ctx)
				return err
			}

			rows = rows[:0]
		}
	}

	if err := reader.Close(ctx); err != nil {
		return err
	}

	if err := printRows(); err != nil {
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

func (t *PgsqlTarget) copyTableData(
	ctx context.Context,
	table *schema.Table,
	reader dialect.TableDataReader,
	action dialect.CopyAction,
) error {
	tablesMap, err := t.GetTables(ctx)
	if err != nil {
		return err
	}

	var tempTable schema.Table

	copyData := func(source, target *schema.Table) error {
		tableName := translateIdentifier(target.Name)

		cols := schema.UpdateableColumns(target)

		colNames := make([]string, len(cols))
		for n, col := range cols {
			colNames[n] = translateIdentifier(col.Name)
		}

		if err := reader.Open(ctx, source.Name, cols); err != nil {
			return err
		}

		var copyRows [][]any
		CopyBatchSize := 10000

		copy := func() error {
			if len(copyRows) > 0 {
				fmt.Printf("%s - copy columns %v\n", source.Name, colNames)

				count, err := t.conn.CopyFrom(ctx, pgx.Identifier{tableName}, colNames, pgx.CopyFromRows(copyRows))
				if err != nil {
					return err
				}

				if target != &tempTable {
					fmt.Printf("%s - copy data - copied %d rows\n", source.Name, count)
				}

				t.conn.QueryRow(ctx, fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count)
				fmt.Printf("%s - count %d", source.Name, count)
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

	targetTableName := translateIdentifier(table.Name)

	targetTable, ok := tablesMap[targetTableName]
	if !ok {
		// table missing, nothing to do
		fmt.Printf("%s: copy data - target table missing, nothing to do\n", table.Name)
		return nil
	}

	pkColNames := schema.PrimaryKeyColumns(table)

	empty, err := isTableEmpty(ctx, t.conn, targetTableName)
	if err != nil {
		return err
	}

	if !empty && action == dialect.CopyInsert {
		fmt.Printf("%s - copy data - target table has data, nothing to do\n", table.Name)
		return nil
	}

	// emtpy table, just copy the data
	if empty {
		fmt.Printf("%s - copy data - target table empty, copying data\n", table.Name)
		if err := copyData(table, targetTable); err != nil {
			return err
		}

		return nil
	}

	// overwrite, truncate data, copy data
	// merge requires primary key, use overwrite if no primary key
	if action == dialect.CopyOverwrite || len(pkColNames) == 0 {
		fmt.Printf("%s - copy data - overwriting data\n", table.Name)
		if err := copyData(table, targetTable); err != nil {
			return err
		}

		return nil
	}

	// merge, use replication, merge new+existing rows, delete rows not in source
	getTempTableName := func() (string, error) {
		retries := 10
		for retries > 0 {
			tempTableName, err := generateTempTableName()
			if err != nil {
				return "", fmt.Errorf("failed to create temp table: %v", err)
			}
			if _, ok := tablesMap[tempTableName]; !ok {
				return tempTableName, nil
			}
			retries--
		}
		return "", fmt.Errorf("failed to create unique temp table name")
	}

	tempTableName, err := getTempTableName()
	if err != nil {
		return err
	}

	tempTable = *table
	tempTable.Name = tempTableName

	sql := CreateTableStatement(&tempTable, t.textType, false)
	_, err = t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %v", err)
	}

	dropTempTable := func() error {
		sql := DropTableStatement(&tempTable)
		_, err := t.conn.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to drop temp table: %v", err)
		}
		return nil
	}

	defer dropTempTable()

	if err := copyData(table, &tempTable); err != nil {
		return err
	}

	matchColNames := make([]string, len(pkColNames))
	for n, col := range pkColNames {
		translatedName := translateIdentifier(col)
		matchColNames[n] = fmt.Sprintf("t.%s = s.%s", translatedName, translatedName)
	}
	matchClause := strings.Join(matchColNames, " and ")

	updateableCols := schema.UpdateableColumns(table)

	var updateClause string
	updateColNames := make([]string, len(updateableCols)-len(pkColNames))
	if len(updateColNames) > 0 {
		n := 0
		for _, col := range updateableCols {
			if !slices.Contains(pkColNames, col.Name) {
				translatedName := translateIdentifier(col.Name)
				updateColNames[n] = fmt.Sprintf("%s = s.%s", translatedName, translatedName)
				n++
			}
		}
		updateClause = fmt.Sprintf("when matched then update set %s", strings.Join(updateColNames, ", "))
	}

	insertColNames := make([]string, len(updateableCols))
	for n, col := range updateableCols {
		translatedName := translateIdentifier(col.Name)
		insertColNames[n] = fmt.Sprintf("%s", translatedName)
	}
	insertClause := strings.Join(insertColNames, ", ")

	insertValuesColNames := make([]string, len(updateableCols))
	for n, col := range updateableCols {
		translatedName := translateIdentifier(col.Name)
		insertValuesColNames[n] = fmt.Sprintf("s.%s", translatedName)
	}
	insertValuesClause := strings.Join(insertValuesColNames, ", ")

	sql = fmt.Sprintf("merge into %s as t using %s AS s on %s %s when not matched then insert (%s) values (%s)", targetTableName, tempTable.Name, matchClause, updateClause, insertClause, insertValuesClause)

	var tag pgconn.CommandTag

	tag, err = t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to merge table (insert/update) %s: %v", targetTableName, err)
	}

	fmt.Printf("%s - copy data - merged %d rows\n", table.Name, tag.RowsAffected())

	// delete when not matched by source
	sql = fmt.Sprintf("delete from %s as t where not exists (select 1 from %s as s where %s);", targetTableName, tempTable.Name, matchClause)

	tag, err = t.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to merge table (delete) %s: %v", targetTableName, err)
	}

	fmt.Printf("%s - copy data - deleted %d rows\n", table.Name, tag.RowsAffected())

	return nil
}

func (t *PgsqlTarget) execSeqReset(ctx context.Context) error {
	var missing bool
	sql := "select not exists (select 1 from pg_proc where proname = $1 and pronamespace = $2::regnamespace)"
	if err := t.conn.QueryRow(ctx, sql, "seq_field_max_value", "public").Scan(&missing); err != nil {
		return fmt.Errorf("failed to check seq_field_max_value function: %v", err)
	}

	if missing {
		if _, err := t.conn.Exec(ctx, CreateSeqResetFnStatement()); err != nil {
			return fmt.Errorf("failed to create seq_field_max_value function: %v", err)
		}
	}

	if _, err := t.conn.Exec(ctx, ExecSeqResetFnStatement()); err != nil {
		return fmt.Errorf("failed reset sequence values: %v", err)
	}

	return nil
}

func (t *PgsqlTarget) writeTablesSchema(tables []*schema.Table, recreate bool) error {
	fmt.Fprintf(t.out, "/* --------------------- TABLES --------------------- */\n\n")

	if t.textType == "citext" {
		fmt.Fprintf(t.out, "create extension if not exists citext;\n\n")
	}

	for _, table := range tables {
		if err := t.writeTableSchema(table, recreate); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeTableSchema(table *schema.Table, recreate bool) error {
	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n",
		table.Name,
	)

	if recreate {
		drop := DropTableStatement(table)

		fmt.Fprintf(
			t.out,
			"%s\n\n",
			drop,
		)
	}

	create := CreateTableStatement(table, t.textType, recreate)

	fmt.Fprintf(
		t.out,
		"%s\n\n",
		create,
	)

	return nil
}

func (t *PgsqlTarget) createTablesSchema(
	ctx context.Context,
	tables []*schema.Table,
	recreate bool,
) error {
	if _, err := t.conn.Exec(ctx, "create extension if not exists citext;\n\n"); err != nil {
		return err
	}

	for _, table := range tables {
		if err := t.createTableSchema(ctx, table, recreate); err != nil {
			return err
		}
	}

	return nil
}

func (t *PgsqlTarget) createTableSchema(
	ctx context.Context,
	table *schema.Table,
	recreate bool,
) error {
	if recreate {
		sql := DropTableStatement(table)
		if _, err := t.conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to drop table %s: %v", table.Name, err)
		}
	}

	sql := CreateTableStatement(table, t.textType, recreate)
	if _, err := t.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("failed to create table %s: %v", table.Name, err)
	}

	fmt.Printf("%s - create table\n", table.Name)

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
		return fmt.Errorf("failed to create index %v - %s: %v", index, sql, err)
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
		return fmt.Errorf("failed to create foreign key %v - %s: %v", key, sql, err)
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

func (t *PgsqlTarget) writeTriggers(triggers []*schema.Trigger) error {
	fmt.Fprintf(t.out, "/* --------------------- TRIGGERS--------------------- */\n\n")

	for _, trigger := range triggers {
		if err := t.writeTrigger(trigger); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeTrigger(trigger *schema.Trigger) error {
	drop := DropTriggerStatement(trigger)
	create := CreateTriggerStatement(trigger)

	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n%s\n\n%s\n\n",
		trigger.Name,
		drop,
		create,
	)

	return nil
}

func (t *PgsqlTarget) writeProcedures(procedures []*schema.Procedure) error {
	fmt.Fprintf(t.out, "/* --------------------- PROCEDURES --------------------- */\n\n")

	for _, procedure := range procedures {
		if err := t.writeProcedure(procedure); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeProcedure(procedure *schema.Procedure) error {
	drop := DropProcedureStatement(procedure)
	create := CreateProcedureStatement(procedure)

	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n%s\n\n%s\n\n",
		procedure.Name,
		drop,
		create,
	)

	return nil
}

func (t *PgsqlTarget) writeFunctions(functions []*schema.Function) error {
	fmt.Fprintf(t.out, "/* --------------------- FUNCTIONS --------------------- */\n\n")

	for _, function := range functions {
		if err := t.writeFunction(function); err != nil {
			return err
		}
	}

	fmt.Fprint(t.out, "\n")

	return nil
}

func (t *PgsqlTarget) writeFunction(function *schema.Function) error {
	drop := DropFunctionStatement(function)
	create := CreateFunctionStatement(function)

	fmt.Fprintf(
		t.out,
		"/* -- %s -- */\n%s\n\n%s\n\n",
		function.Name,
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

func readIndexes(ctx context.Context, conn *pgx.Conn) ([]*schema.Index, error) {
	q := `SELECT
        tbl.relname AS table_name,
        idx.relname AS index_name,
        COALESCE(att.attname, '') AS column_name,
        ord.n AS index_column_id,
        (ord.n > ix.indnkeyatts) AS is_included,
        ix.indisprimary AS is_primary,
        COALESCE(con.contype = 'u', false) AS is_unique_constraint,
        ix.indisunique AS is_unique,
        COALESCE(pg_get_expr(ix.indpred, ix.indrelid), '') AS filter
    FROM pg_index ix
    JOIN pg_class tbl ON tbl.oid = ix.indrelid
    JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
    JOIN pg_class idx ON idx.oid = ix.indexrelid
    LEFT JOIN pg_constraint con ON con.conindid = ix.indexrelid AND con.contype IN ('u','p')
    LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, n) ON TRUE
    LEFT JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = ord.attnum
    WHERE tbl.relkind = 'r'
      AND ns.nspname NOT IN ('pg_catalog','information_schema')
    ORDER BY tbl.relname ASC, idx.relname ASC, ord.n ASC`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		result    []*schema.Index
		cur       *schema.Index
		lastTable string
		lastIndex string
	)

	for rows.Next() {
		var (
			tableName, indexName, columnName string
			indexColumnID                    int
			isIncluded                       bool
			isPrimary                        bool
			isUniqueConstraint               bool
			isUnique                         bool
			filter                           string
		)

		if err := rows.Scan(
			&tableName,
			&indexName,
			&columnName,
			&indexColumnID,
			&isIncluded,
			&isPrimary,
			&isUniqueConstraint,
			&isUnique,
			&filter,
		); err != nil {
			return nil, err
		}

		if indexName != lastIndex || tableName != lastTable {
			if cur != nil {
				result = append(result, cur)
			}

			var it schema.IndexType
			switch {
			case isPrimary:
				it = schema.IndexTypePrimaryKey
			case isUniqueConstraint:
				it = schema.IndexTypeUniqueConstraint
			case isUnique:
				it = schema.IndexTypeUnique
			default:
				it = schema.IndexTypeNonUnique
			}

			cur = &schema.Index{
				Table:          tableName,
				Name:           indexName,
				Columns:        []string{},
				IncludeColumns: []string{},
				Filter:         strings.TrimSpace(filter),
				IndexType:      it,
			}

			lastTable = tableName
			lastIndex = indexName
		}

		if columnName != "" {
			if isIncluded {
				cur.IncludeColumns = append(cur.IncludeColumns, columnName)
			} else {
				cur.Columns = append(cur.Columns, columnName)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if cur != nil {
		result = append(result, cur)
	}

	return result, nil
}

func readForeignKeys(ctx context.Context, conn *pgx.Conn) ([]*schema.ForeignKey, error) {
	q := `SELECT
        c.conname AS key_name,
        pt.relname AS parent_table,
        pcol.attname AS parent_column,
        rt.relname AS referenced_table,
        rcol.attname AS referenced_column,
        ord.n AS constraint_column_id
    FROM pg_constraint c
    JOIN pg_class pt ON pt.oid = c.conrelid
    JOIN pg_namespace ns ON ns.oid = pt.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS ord(attnum, n) ON TRUE
    JOIN pg_attribute pcol ON pcol.attrelid = pt.oid AND pcol.attnum = ord.attnum
    JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS ord2(attnum, n) ON ord2.n = ord.n
    JOIN pg_attribute rcol ON rcol.attrelid = rt.oid AND rcol.attnum = ord2.attnum
    WHERE c.contype = 'f'
      AND ns.nspname NOT IN ('pg_catalog','information_schema')
    ORDER BY pt.relname ASC, c.oid ASC, ord.n ASC`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		result                    []*schema.ForeignKey
		curKey                    string
		curParent, curRef         string
		curParentCols, curRefCols []string
	)

	for rows.Next() {
		var (
			keyName, pTable, pColumn, rTable, rColumn string
			pos                                       int
		)
		if err := rows.Scan(&keyName, &pTable, &pColumn, &rTable, &rColumn, &pos); err != nil {
			return nil, err
		}

		if keyName != curKey {
			if curKey != "" {
				result = append(result, &schema.ForeignKey{
					Name:              curKey,
					Table:             curParent,
					Columns:           curParentCols,
					ReferencedTable:   curRef,
					ReferencedColumns: curRefCols,
				})
			}

			curKey = keyName
			curParent = pTable
			curRef = rTable
			curParentCols = []string{}
			curRefCols = []string{}
		}

		curParentCols = append(curParentCols, pColumn)
		curRefCols = append(curRefCols, rColumn)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if curKey != "" {
		result = append(result, &schema.ForeignKey{
			Name:              curKey,
			Table:             curParent,
			Columns:           curParentCols,
			ReferencedTable:   curRef,
			ReferencedColumns: curRefCols,
		})
	}

	return result, nil
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
	return fmt.Sprintf("drop table if exists %s cascade;", table.Name)
}

func CreateTableStatement(
	table *schema.Table,
	textType string,
	recreate bool,
) string {
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

	var ifExists = "if not exists "
	if recreate {
		ifExists = ""
	}

	sql := fmt.Sprintf(
		"create table %s%s\n(\n%s\n);",
		ifExists,
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

func DropTriggerStatement(trigger *schema.Trigger) string {
	return fmt.Sprintf("drop trigger if exists %s;", trigger.Name)
}

func CreateTriggerStatement(trigger *schema.Trigger) string {
	sql := translateMarkers(trigger.Definition)

	return sql
}

func DropProcedureStatement(procedure *schema.Procedure) string {
	return fmt.Sprintf("drop procedure if exists %s;", procedure.Name)
}

func CreateProcedureStatement(procedure *schema.Procedure) string {
	sql := translateMarkers(procedure.Definition)
	return sql
}

func DropFunctionStatement(function *schema.Function) string {
	return fmt.Sprintf("drop function if exists %s;", function.Name)
}

func CreateFunctionStatement(function *schema.Function) string {
	sql := translateMarkers(function.Definition)
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

// VerifyDataIntegrity checks each foreign key has a parent row
// Returns a slice of human-readable violations
func (t *PgsqlTarget) VerifyDataIntegrity(ctx context.Context, tables []*schema.Table) ([]string, error) {
	if t.conn == nil {
		return nil, nil
	}

	var violations []string

	for _, tbl := range tables {
		for _, fk := range tbl.ForeignKeyes {
			if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) {
				continue
			}

			childTable := escapeIdentifier(translateIdentifier(fk.Table))
			parentTable := escapeIdentifier(translateIdentifier(fk.ReferencedTable))

			onConds := make([]string, len(fk.Columns))
			notNulls := make([]string, len(fk.Columns))
			for i := range fk.Columns {
				c := escapeIdentifier(translateIdentifier(fk.Columns[i]))
				r := escapeIdentifier(translateIdentifier(fk.ReferencedColumns[i]))
				onConds[i] = fmt.Sprintf("t.%s = p.%s", c, r)
				notNulls[i] = fmt.Sprintf("t.%s is not null", c)
			}

			on := strings.Join(onConds, " and ")
			nn := strings.Join(notNulls, " and ")

			sql := fmt.Sprintf(
				"select count(*) from %s t where %s and not exists (select 1 from %s p where %s)",
				childTable, nn, parentTable, on,
			)

			var missingCount int64
			if err := t.conn.QueryRow(ctx, sql).Scan(&missingCount); err != nil {
				return nil, fmt.Errorf("data integrity check failed - %s -> %s: %v", fk.Table, fk.ReferencedTable, err)
			}

			if missingCount > 0 {
				violations = append(violations,
					fmt.Sprintf(
						"%s -> %s (%v -> %v): %d orphan row/s",
						fk.Table,
						fk.ReferencedTable,
						fk.Columns,
						fk.ReferencedColumns,
						missingCount,
					))
			}
		}
	}

	return violations, nil
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

func generateTempTableName() (string, error) {
	randomBytes := make([]byte, 20)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	s := base64.URLEncoding.EncodeToString(randomBytes)
	s = strings.TrimRight(s, "=")
	s = strings.ReplaceAll(s, "-", "")
	tableName := "temp_" + s

	return tableName, nil
}

func isTableEmpty(ctx context.Context, conn *pgx.Conn, table string) (bool, error) {
	sql := fmt.Sprintf("select not exists (select 1 from %s)", table)
	var empty bool
	if err := conn.QueryRow(ctx, sql).Scan(&empty); err != nil {
		return false, err
	}
	return empty, nil
}
