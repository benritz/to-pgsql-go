package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"benritz/topgsql/internal/dialect/mssql"
	"benritz/topgsql/internal/dialect/pgsql"
	"benritz/topgsql/internal/schema"
)

var (
	sourceUrl     string
	targetUrl     string
	incData       bool
	incTables     bool
	incFunctions  bool
	incTriggers   bool
	incProcedures bool
	incViews      bool
	textType      string
	dataBatchSize int
)

// Precompiled regexes for view normalization
var (
	reAsSingleAlias = regexp.MustCompile(`(?i)\bas\s*'([^']+)'`)
	reAsDoubleAlias = regexp.MustCompile(`(?i)\bas\s*"([^"]+)"`)
	reNLiteral      = regexp.MustCompile(`(?i)\bN'((?:[^']|'')*)'`)
	reConcatLeft    = regexp.MustCompile(`(?i)(N?'(?:[^']|'')*')\s*\+\s*`)
	reConcatRight   = regexp.MustCompile(`(?i)\s*\+\s*(N?'(?:[^']|'')*')`)
	reTrailingGo    = regexp.MustCompile(`(?mi)(?:\r?\n)[ \t]*GO[ \t]*\s*$`)
)

func values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func main() {
	flag.StringVar(&sourceUrl, "source", "", "Source database connection URL")
	flag.StringVar(&targetUrl, "target", "", "Target file or database connection URL")
	flag.StringVar(&textType, "textType", "citext", "How to convert the text column schema. Either text, citext or varchar (default).")
	flag.BoolVar(&incData, "incData", false, "Include table data")
	flag.BoolVar(&incTables, "incTables", false, "Include tables schema")
	flag.BoolVar(&incFunctions, "incFunctions", false, "Include functions")
	flag.BoolVar(&incProcedures, "incProcedures", false, "Include procedures")
	flag.BoolVar(&incTriggers, "incTriggers", false, "Include triggers")
	flag.BoolVar(&incViews, "incViews", false, "Include views")
	flag.IntVar(&dataBatchSize, "dataBatchSize", 100, "Batch size for data inserts")
	flag.Parse()

	if sourceUrl == "" {
		fmt.Fprintln(os.Stderr, "Missing the source database connection URL")
		os.Exit(1)
	}

	ctx := context.Background()

	source, err := mssql.NewMssqlSource(sourceUrl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect to source:", err)
		os.Exit(1)
	}
	defer source.Close()

	target, err := pgsql.NewPgsqlTarget(ctx, targetUrl, textType)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect to target:", err)
		os.Exit(1)
	}
	defer target.Close(ctx)

	reader, err := source.NewTableDataReader(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create table data reader:", err)
		os.Exit(1)
	}

	tablesMap, err := source.GetTables(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get tables:", err)
		os.Exit(1)
	}

	table, ok := tablesMap["ibt_component_type"]
	if !ok {
		fmt.Fprintln(os.Stderr, "Table IBT_ITEM_TYPE not found in source database")
		os.Exit(1)
	}

	if err := target.CopyTable(ctx, table, reader); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to copy table data:", err)
		os.Exit(1)
	}

	// if incTables || incData {
	// 	tables, err := source.GetTables(ctx)
	// 	if err != nil {
	// 		fmt.Fprintln(os.Stderr, err)
	// 		os.Exit(1)
	// 	}
	//
	// 	if incData {
	// 		setReplicationOn(ctx, conn)
	// 		err := copyAllTableData(ctx, db, conn, tables)
	// 		setReplicationOff(ctx, conn)
	//
	// 		if err != nil {
	// 			fmt.Fprintln(os.Stderr, "Failed to copy table data: ", err)
	// 			os.Exit(1)
	// 		}
	// 	}
	// }

	if incTables || incData {
		tablesMap, err := source.GetTables(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		tables := values(tablesMap)

		indexes, err := source.GetIndexes(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		foreignKeys, err := source.GetForeignKeys(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if incTables {
			if err := target.CreateTables(tables); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}

		// if incData {
		// 	if err := writeAllTableData(db, out, tables); err != nil {
		// 		fmt.Fprintln(os.Stderr, err)
		// 		os.Exit(1)
		// 	}
		// }
		//
		if incTables {
			if err := target.CreateIndexes(indexes); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			if err := target.CreateForeignKeys(foreignKeys); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	// if incFunctions {
	// 	if err := writeFunctions(db, out); err != nil {
	// 		fmt.Fprintln(os.Stderr, err)
	// 		os.Exit(1)
	// 	}
	// }
	//
	// if incProcedures {
	// 	if err := writeProcedures(db, out); err != nil {
	// 		fmt.Fprintln(os.Stderr, err)
	// 		os.Exit(1)
	// 	}
	// }
	//
	// if incViews {
	// 	if err := writeViews(db, out); err != nil {
	// 		fmt.Fprintln(os.Stderr, err)
	// 		os.Exit(1)
	// 	}
	// }
	//
	// if incTriggers {
	// 	if err := writeTriggers(db, out); err != nil {
	// 		fmt.Fprintln(os.Stderr, err)
	// 		os.Exit(1)
	// 	}
	// }
	// }
}

func writeTableData(db *sql.DB, out io.Writer, table string) error {
	query := fmt.Sprintf("select * from %s", table)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	n := 0
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		valStrs := make([]string, len(cols))

		for i, val := range values {
			switch v := val.(type) {
			case nil:
				valStrs[i] = "NULL"
			case []byte:
				t := strings.ToLower(colTypes[i].DatabaseTypeName())
				switch t {
				case "decimal", "numeric", "money", "smallmoney":
					valStrs[i] = strings.TrimSpace(string(v))
				case "uniqueidentifier":
					if len(v) == 16 {
						u := uuidFromBytes(v)
						valStrs[i] = fmt.Sprintf("'%s'", u)
					} else if len(v) == 0 {
						valStrs[i] = "NULL"
					} else {
						valStrs[i] = fmt.Sprintf("'%s'", strings.ToLower(strings.TrimSpace(string(v))))
					}
				default:
					if len(v) == 0 {
						valStrs[i] = "'\\x'"
					} else {
						valStrs[i] = fmt.Sprintf("'\\x%x'", v)
					}
				}
			case string:
				t := strings.ToLower(colTypes[i].DatabaseTypeName())
				switch t {
				case "decimal", "numeric", "money", "smallmoney":
					valStrs[i] = strings.TrimSpace(v)
				case "uniqueidentifier":
					valStrs[i] = fmt.Sprintf("'%s'", strings.ToLower(strings.TrimSpace(v)))
				default:
					valStrs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
				}
			case time.Time:
				valStrs[i] = fmt.Sprintf("'%s'", v.UTC().Format("2006-01-02 15:04:05.999999Z07:00"))
			default:

				t := strings.ToLower(colTypes[i].DatabaseTypeName())
				switch t {
				case "decimal", "numeric", "money", "smallmoney":
					valStrs[i] = fmt.Sprintf("%v", v)
				case "uniqueidentifier":
					valStrs[i] = fmt.Sprintf("'%v'", v)
				default:
					valStrs[i] = fmt.Sprintf("%v", v)
				}
			}
		}

		if dataBatchSize == 1 {
			fmt.Fprintf(out, "insert into %s values (%s);\n", table, strings.Join(valStrs, ", "))
		} else {
			if n%dataBatchSize == 0 {
				if n != 0 {
					fmt.Fprintf(out, ";\n\n")
				}
				fmt.Fprintf(out, "insert into %s values (%s)", table, strings.Join(valStrs, ", "))
			} else {
				fmt.Fprintf(out, "\n,(%s)", strings.Join(valStrs, ", "))
			}
		}
		n++
	}

	if n > 0 {
		fmt.Fprintf(out, ";\n\n")
	}

	return nil
}

func writeAllTableData(db *sql.DB, out io.Writer, tables []schema.Table) error {
	fmt.Fprintf(out, "set session_replication_role = 'replica';")

	for _, t := range tables {
		if err := writeTableData(db, out, t.Name); err != nil {
			return err
		}
	}

	writeSeqReset(out)

	fmt.Fprintf(out, "set session_replication_role = 'origin';")

	return nil
}

func writeSeqReset(out io.Writer) {
	fmt.Fprintf(out, `create or replace function seq_field_max_value(seq_oid oid) returns bigint
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
$$;`)

	fmt.Fprintf(out, "-- set any sequence to the maximum value of the sequence's field\n")
	fmt.Fprintf(out, "select relname, setval(oid, seq_field_max_value(oid)) from pg_class where relkind = 'S';\n\n")
}

func writeIndexes(db *sql.DB, out io.Writer, table *schema.Table) error {
	sql := "select t.name as table_name, c.name as column_name, i.name as index_name, ic.index_column_id, i.is_primary_key, i.is_unique_constraint, i.is_unique from sys.indexes i, sys.index_columns ic, sys.tables t, sys.columns c where i.object_id = t.object_id and ic.object_id = t.object_id and ic.index_id = i.index_id and t.object_id = c.object_id and ic.column_id = c.column_id"
	args := []any{}

	if table != nil {
		sql += " and t.name = @p1 "
		args = append(args, table.Name)
	}

	sql += " order by t.name asc, t.object_id asc, i.index_id asc, ic.index_column_id asc"

	rows, err := db.Query(
		sql,
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		lastTable, lastIndex               string
		columns                            []string
		isPrimary, isUniqueConst, isUnique bool
		first                              bool = true
	)

	for rows.Next() {
		var tableName, columnName, indexName string
		var indexColumnID int
		var pk, uc, uq bool
		if err := rows.Scan(&tableName, &columnName, &indexName, &indexColumnID, &pk, &uc, &uq); err != nil {
			return err
		}
		if indexColumnID == 1 {
			if !first {
				if isPrimary {
					fmt.Fprintf(out, "alter table %s add constraint %s primary key (%s);\n", lastTable, lastIndex, strings.Join(columns, ", "))
				} else if isUniqueConst {
					fmt.Fprintf(out, "alter table %s add constraint %s unique (%s);\n", lastTable, lastIndex, strings.Join(columns, ", "))
				} else if isUnique {
					fmt.Fprintf(out, "create unique index %s on %s (%s);\n", lastIndex, lastTable, strings.Join(columns, ", "))
				} else {
					fmt.Fprintf(out, "create index %s on %s (%s);\n", lastIndex, lastTable, strings.Join(columns, ", "))
				}

				if lastTable != tableName {
					fmt.Fprintf(out, "\n")
				}
			}
			lastTable = tableName
			lastIndex = indexName
			isPrimary = pk
			isUniqueConst = uc
			isUnique = uq
			columns = []string{}
			first = false
		}
		columns = append(columns, columnName)
	}

	if !first {
		if isPrimary {
			fmt.Fprintf(out, "alter table %s add constraint %s primary key (%s);\n", lastTable, lastIndex, strings.Join(columns, ", "))
		} else if isUniqueConst {
			fmt.Fprintf(out, "alter table %s add constraint %s unique (%s);\n", lastTable, lastIndex, strings.Join(columns, ", "))
		} else if isUnique {
			fmt.Fprintf(out, "create unique index %s on %s (%s);\n", lastIndex, lastTable, strings.Join(columns, ", "))
		} else {
			fmt.Fprintf(out, "create index %s on %s (%s);\n", lastIndex, lastTable, strings.Join(columns, ", "))
		}
		fmt.Fprintf(out, "\n")
	}

	return nil
}

func writeForeignKeys(db *sql.DB, out io.Writer, table *schema.Table) error {
	if table == nil {
		fmt.Fprintf(out, "/* --------------------- FOREIGN KEYS --------------------- */\n\n")
	}

	sql := "select fk.name as key_name, t.name as parent_table, c.name as parent_column, rt.name as referenced_table, rc.name as referenced_column, fkc.constraint_column_id as constraint_column_id from sys.tables t, sys.tables rt, sys.columns c, sys.columns rc, sys.foreign_keys fk, sys.foreign_key_columns fkc where fk.object_id = fkc.constraint_object_id and t.object_id = fk.parent_object_id and fkc.parent_column_id = c.column_id and c.object_id = t.object_id and rt.object_id = fk.referenced_object_id and fkc.referenced_column_id = rc.column_id and rc.object_id = rt.object_id"
	args := []any{}

	if table != nil {
		sql += " and t.name = @p1"
		args = append(args, table.Name)
	}

	sql += " order by fk.object_id asc, fkc.constraint_column_id asc"

	rows, err := db.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		key, parentTable, referencedTable string
		parentColumns, referencedColumns  []string
		first                             bool = true
	)

	for rows.Next() {
		var keyName, pTable, pColumn, rTable, rColumn string
		var constraintColumnID int
		if err := rows.Scan(&keyName, &pTable, &pColumn, &rTable, &rColumn, &constraintColumnID); err != nil {
			return err
		}
		if constraintColumnID == 1 {
			if !first {
				fmt.Fprintf(out, "alter table %s add constraint %s foreign key (%s) references %s (%s);\n", parentTable, key, strings.Join(parentColumns, ", "), referencedTable, strings.Join(referencedColumns, ", "))
				if parentTable != pTable {
					fmt.Fprintf(out, "\n")
				}
			}
			key = keyName
			parentTable = pTable
			referencedTable = rTable
			parentColumns = []string{}
			referencedColumns = []string{}
			first = false
		}
		parentColumns = append(parentColumns, pColumn)
		referencedColumns = append(referencedColumns, rColumn)
	}

	if !first {
		fmt.Fprintf(out, "alter table %s add constraint %s foreign key (%s) references %s (%s);\n", parentTable, key, strings.Join(parentColumns, ", "), referencedTable, strings.Join(referencedColumns, ", "))
		fmt.Fprintf(out, "\n")
	}

	return nil
}

func writeFunctions(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- FUNCTIONS --------------------- */\n\n")
	fmt.Fprintf(out, "/* -- These functions contain T-SQL and MUST be rewritten for PGSQL -- */\n\n")
	return writeCompiledObject(db, out, "FN")
}

func writeProcedures(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- PROCEDURES --------------------- */\n\n")
	fmt.Fprintf(out, "/* -- These procedures contain T-SQL and MUST be rewritten for PGSQL -- */\n\n")
	return writeCompiledObject(db, out, "P")
}

func writeViews(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- VIEWS --------------------- */\n\n")
	fmt.Fprintf(out, "/* -- These views may contain T-SQL and may need to be rewritten for PGSQL -- */\n\n")
	return writeCompiledObject(db, out, "V")
}

func writeTriggers(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- TRIGGERS --------------------- */\n\n")
	fmt.Fprintf(out, "/* -- These triggers contain T-SQL and MUST be rewritten for PGSQL -- */\n\n")
	return writeCompiledObject(db, out, "TR")
}

// Helper for compiled objects (functions, procedures, views, triggers)
func writeCompiledObject(db *sql.DB, out io.Writer, objType string) error {
	rows, err := db.Query("select c.text, c.colid from sysobjects o, syscomments c where c.id = o.id and o.type = @p1 order by o.name, c.colid asc", objType)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		buf     strings.Builder
		started bool
	)

	flush := func() error {
		if !started {
			return nil
		}
		content := buf.String()
		content = normalizeLineEndings(content)
		if objType == "V" {
			content = normalizeViewText(content)
			content = strings.TrimSpace(content)
			fmt.Fprint(out, content)
			if strings.HasSuffix(strings.TrimSpace(content), ";") {
				fmt.Fprint(out, "\n\n")
			} else {
				fmt.Fprint(out, ";\n\n")
			}
		} else {
			fmt.Fprint(out, content)
			fmt.Fprintf(out, ";\nGO\n\n")
		}
		buf.Reset()
		return nil
	}

	for rows.Next() {
		var text string
		var colid int
		if err := rows.Scan(&text, &colid); err != nil {
			return err
		}
		if colid == 1 {
			if err := flush(); err != nil {
				return err
			}
			started = true
		}
		buf.WriteString(strings.TrimSpace(text))
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}

func normalizeLineEndings(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func normalizeViewText(s string) string {

	s = reNLiteral.ReplaceAllString(s, `'${1}'`)

	s = reAsSingleAlias.ReplaceAllString(s, "AS $1")

	s = reAsDoubleAlias.ReplaceAllString(s, "AS $1")

	s = reConcatLeft.ReplaceAllString(s, "$1 || ")
	s = reConcatRight.ReplaceAllString(s, " || $1")

	s = reTrailingGo.ReplaceAllString(s, "\n")

	return s
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

func quotedIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func copyTableDataGeneric(src *sql.DB, dst *sql.DB, table string) error {
	query := fmt.Sprintf("select * from %s", table)
	rows, err := src.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	quotedCols := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quotedIdentifier(c)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	sql := fmt.Sprintf(
		"insert into %s (%s) values (%s)",
		quotedIdentifier(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := dst.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sql)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	args := make([]any, len(cols))

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			_ = tx.Rollback()
			return err
		}

		for i := range values {
			dbType := strings.ToLower(colTypes[i].DatabaseTypeName())
			// Construct a minimal schema.DataType from the raw db type for generic path
			var dt schema.DataType
			switch dbType {
			case "decimal", "numeric":
				dt.Kind = schema.KindNumeric
			case "money", "smallmoney":
				dt.Kind = schema.KindMoney
			case "uniqueidentifier":
				dt.Kind = schema.KindUUID
			case "bit":
				dt.Kind = schema.KindBool
			default:
				dt.Kind = schema.KindUnknown
				dt.Raw = dbType
			}

			args[i] = pgsql.ConvertValue(values[i], dt)
		}

		if _, err := stmt.Exec(args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func updateableColumns(table schema.Table) []schema.Column {
	cols := []schema.Column{}
	for _, col := range table.Columns {
		if !col.IsComputed {
			cols = append(cols, col)
		}
	}
	return cols
}

func selectClause(cols []schema.Column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return strings.Join(names, ", ")
}
