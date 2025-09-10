package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
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

type Column struct {
	ColumnID   int
	Name       string
	MaxLength  int
	Precision  int
	Scale      int
	IsNullable bool
	IsAutoInc  bool
	Type       string
	Default    string
}

type Table struct {
	Name    string
	Columns []Column
}

func main() {
	flag.StringVar(&textType, "textType", "citext", "How to convert the text column types. Either text, citext or varchar (default).")
	flag.BoolVar(&incData, "incData", false, "Include table data")
	flag.BoolVar(&incTables, "incTables", false, "Include tables schema")
	flag.BoolVar(&incFunctions, "incFunctions", false, "Include functions")
	flag.BoolVar(&incProcedures, "incProcedures", false, "Include procedures")
	flag.BoolVar(&incTriggers, "incTriggers", false, "Include triggers")
	flag.BoolVar(&incViews, "incViews", false, "Include views")
	flag.IntVar(&dataBatchSize, "dataBatchSize", 100, "Batch size for data inserts")
	flag.Parse()

	args := flag.Args()

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Missing the connection URL.")
		os.Exit(1)
	}

	url := args[0]

	var out *os.File
	if len(args) == 1 {
		out = os.Stdout
	} else {
		outputFile := args[1]
		f, err := os.Create(outputFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to create output file:", err)
			os.Exit(1)
		}
		out = f
		// out.WriteString("\ufeff") // UTF-8 BOM for Windows
		defer out.Close()
	}

	db, err := sql.Open("sqlserver", url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect to source:", err)
		os.Exit(1)
	}
	defer db.Close()

	tables, err := readTables(db)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if incTables {
		if err := writeAllTableSchemas(db, out, tables, !incData); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incData {
		if err := writeAllTableData(db, out, tables); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if err := writeIndexes(db, out, nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incTables {
		if err := writeForeignKeys(db, out, nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incFunctions {
		if err := writeFunctions(db, out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incProcedures {
		if err := writeProcedures(db, out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incViews {
		if err := writeViews(db, out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if incTriggers {
		if err := writeTriggers(db, out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func readTables(db *sql.DB) ([]Table, error) {
	rows, err := db.Query(
		"select t.name as table_name, c.column_id, c.name as column_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type, d.definition " +
			"from sys.tables t " +
			"join sys.columns c on t.object_id = c.object_id " +
			"join sys.types ty on c.user_type_id = ty.user_type_id " +
			"left join sys.default_constraints d on d.parent_object_id = c.object_id and d.parent_column_id = c.column_id " +
			"order by t.name asc, t.object_id asc, c.column_id asc",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []Table{}
	var lastTable string
	var columns []Column
	for rows.Next() {
		var tableName, columnName, colType string
		var columnID, maxLength, precision, scale int
		var isNullable, isAutoInc bool
		var def sql.NullString

		if err := rows.Scan(&tableName, &columnID, &columnName, &maxLength, &precision, &scale, &isNullable, &isAutoInc, &colType, &def); err != nil {
			return nil, err
		}

		if columnID == 1 {
			if lastTable != "" {
				tables = append(tables, Table{Name: lastTable, Columns: columns})
			}
			lastTable = tableName
			columns = []Column{}
		}

		defaultVal := ""
		if !isAutoInc && def.Valid {
			defaultVal = translateDefault(colType, def.String)
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(defaultVal)), "next value for ") {
				isAutoInc = true
				defaultVal = ""
			}
		}

		columns = append(columns, Column{
			ColumnID:   columnID,
			Name:       columnName,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
			IsAutoInc:  isAutoInc,
			Type:       colType,
			Default:    defaultVal,
		})
	}
	if lastTable != "" {
		tables = append(tables, Table{Name: lastTable, Columns: columns})
	}
	return tables, nil
}

// stripEnclosingParens removes full wrapping parentheses like ((...))
// by stripping one outer layer at a time only when that pair encloses
// the entire expression.
func stripEnclosingParens(s string) string {
	s = strings.TrimSpace(s)
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		depth := 0
		endAt := -1
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 {
				endAt = i
				break
			}
		}
		if endAt != len(s)-1 {
			break
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

func translateDefault(colType, def string) string {
	if def == "" {
		return ""
	}
	v := stripEnclosingParens(def)

	// Normalize NVARCHAR literal prefix: N'...'
	if strings.HasPrefix(v, "N'") && len(v) >= 3 {
		v = "'" + v[2:]
	}

	// MSSQL datetime-now equivalents -> Postgres
	if strings.EqualFold(v, "getutcdate()") {
		v = "now() at time zone 'utc'"
	} else if strings.EqualFold(v, "getdate()") || strings.EqualFold(v, "sysdatetime()") || strings.EqualFold(v, "current_timestamp") {
		v = "now()"
	}

	// bit -> boolean mapping
	if strings.EqualFold(colType, "bit") {
		unquoted := v
		if len(unquoted) >= 2 && unquoted[0] == '\'' && unquoted[len(unquoted)-1] == '\'' {
			unquoted = unquoted[1 : len(unquoted)-1]
		}
		switch unquoted {
		case "1", "-1":
			v = "true"
		case "0":
			v = "false"
		}
	}

	return v
}

func writeTableSchema(db *sql.DB, out io.Writer, table Table, incIndexes bool) error {
	columnDefs := ""
	for i, column := range table.Columns {
		if i > 0 {
			columnDefs += ",\n"
		}
		columnDefs += column.Name + " "
		if column.IsAutoInc {
			columnDefs += "serial"
		} else if column.Type == "varchar" || column.Type == "nvarchar" || column.Type == "text" || column.Type == "ntext" || (column.Type == "char" && column.MaxLength > 1) {
			switch textType {
			case "text":
				columnDefs += "text"
			case "citext":
				columnDefs += "citext"
			default:
				if column.MaxLength == -1 {
					columnDefs += "text"
				} else {
					maxLen := column.MaxLength
					if column.Type == "nvarchar" {
						maxLen /= 2
					}
					columnDefs += fmt.Sprintf("varchar(%d)", maxLen)
				}
			}
		} else if column.Type == "datetime" ||
			column.Type == "smalldatetime" ||
			column.Type == "date" ||
			column.Type == "time" ||
			column.Type == "datetime2" {
			columnDefs += "timestamp"
		} else if column.Type == "image" {
			columnDefs += "bytea"
		} else if column.Type == "bit" {
			columnDefs += "boolean"
		} else if column.Type == "uniqueidentifier" {
			columnDefs += "uuid"
		} else if column.Type == "money" {
			columnDefs += "numeric(19, 4)"
		} else {
			columnDefs += column.Type
		}
		if column.Default != "" && !column.IsAutoInc {
			columnDefs += " default " + column.Default
		}
		if !column.IsNullable {
			columnDefs += " not"
		}
		columnDefs += " null"
	}

	fmt.Fprintf(
		out,
		"/* -- %s -- */\ndrop table if exists %s;\n\ncreate table %s\n(\n%s\n);\n\n",
		table.Name,
		table.Name,
		table.Name,
		columnDefs,
	)

	if incIndexes {
		if err := writeIndexes(db, out, &table); err != nil {
			return err
		}
	}

	return nil
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
				// For some drivers, DECIMAL/NUMERIC may arrive as custom types
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

func writeAllTableSchemas(db *sql.DB, out io.Writer, tables []Table, incConstraints bool) error {
	fmt.Fprintf(out, "/* --------------------- TABLES --------------------- */\n\n")

	if textType == "citext" {
		fmt.Fprintf(out, "create extension if not exists citext;\n\n")
	}

	for _, t := range tables {
		if err := writeTableSchema(db, out, t, incConstraints); err != nil {
			return err
		}
	}

	return nil
}

func writeAllTableData(db *sql.DB, out io.Writer, tables []Table) error {
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

func writeIndexes(db *sql.DB, out io.Writer, table *Table) error {
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
				// keep blank line between tables for readability
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

func writeForeignKeys(db *sql.DB, out io.Writer, table *Table) error {
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

// normalizeLineEndings converts CRLF/CR to LF for portable SQL output
func normalizeLineEndings(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

// normalizeViewText applies MSSQL->Postgres textual tweaks for views.
// - Strip single quotes around aliases: AS 'Alias' => AS Alias
// - Strip double quotes around aliases: AS "Alias" => AS Alias
// - Remove Unicode literal prefix: N'...' => '...'
// - Convert string concatenation: 'a' + col => 'a' || col; col + 'b' => col || 'b'
// - Remove trailing batch terminator line GO
func normalizeViewText(s string) string {
	// Remove Unicode string literal prefix N'...'
	s = reNLiteral.ReplaceAllString(s, `'${1}'`)

	// Handle patterns like: AS 'COMMERCE_TYPE' (case-insensitive AS, optional spaces)
	s = reAsSingleAlias.ReplaceAllString(s, "AS $1")

	// Handle patterns like: AS "COMMERCE_TYPE"
	s = reAsDoubleAlias.ReplaceAllString(s, "AS $1")

	// Convert string concatenation when at least one side is a string literal (supports N'...')
	s = reConcatLeft.ReplaceAllString(s, "$1 || ")
	s = reConcatRight.ReplaceAllString(s, " || $1")

	// Remove trailing GO on final line, if present
	s = reTrailingGo.ReplaceAllString(s, "\n")

	return s
}

// uuidFromBytes converts MSSQL uniqueidentifier raw bytes (mixed-endian)
// into a canonical UUID string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
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

func convertValue(val any, dbType string) any {
	switch v := val.(type) {
	case nil:
		return nil
	case []byte:
		switch dbType {
		case "decimal", "numeric", "money", "smallmoney":
			return strings.TrimSpace(string(v))
		case "uniqueidentifier":
			if len(v) == 16 {
				return uuidFromBytes(v)
			} else if len(v) == 0 {
				return nil
			}
			return strings.ToLower(strings.TrimSpace(string(v)))
		case "bit":
			if len(v) > 0 && v[0] != 0 {
				return true
			}
			return false
		default:
			return v
		}
	case string:
		switch dbType {
		case "decimal", "numeric", "money", "smallmoney":
			return strings.TrimSpace(v)
		case "uniqueidentifier":
			return strings.ToLower(strings.TrimSpace(v))
		default:
			return v
		}
	case time.Time:
		return v.UTC()
	default:
		return v
	}
}

func quotedIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func copyTableData(src, dst *sql.DB, table string) error {
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
			args[i] = convertValue(values[i], dbType)
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
