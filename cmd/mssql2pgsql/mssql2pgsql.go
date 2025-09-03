package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	forceCaseInsensitive bool
	dataBatchSize        int
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
}

type Table struct {
	Name    string
	Columns []Column
}

func main() {
	var outputFile string
	flag.BoolVar(&forceCaseInsensitive, "forceCaseInsensitive", true, "Use citext for case-insensitive text columns")
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
		outputFile = args[1]
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

	if err := writeTables(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	writeSeqReset(out)

	if err := writeIndexes(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeForeignKeys(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeFunctions(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeDefaults(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeProcedures(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeViews(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeTriggers(db, out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeTables(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- TABLES --------------------- */\n\n")
	if forceCaseInsensitive {
		fmt.Fprintf(out, "-- enable case insensitive extension\n")
		fmt.Fprintf(out, "create extension citext;\n\n")
	}
	// Query table and column metadata
	rows, err := db.Query("select t.name as table_name, c.column_id, c.name as column_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id order by t.name asc, t.object_id asc, c.column_id asc")
	if err != nil {
		return err
	}
	defer rows.Close()
	tables := []Table{}
	var lastTable string
	var columns []Column
	for rows.Next() {
		var tableName, columnName, colType string
		var columnID, maxLength, precision, scale int
		var isNullable, isIdentity bool
		if err := rows.Scan(&tableName, &columnID, &columnName, &maxLength, &precision, &scale, &isNullable, &isIdentity, &colType); err != nil {
			return err
		}
		if columnID == 1 {
			if lastTable != "" {
				tables = append(tables, Table{Name: lastTable, Columns: columns})
			}
			lastTable = tableName
			columns = []Column{}
		}
		columns = append(columns, Column{ColumnID: columnID, Name: columnName, MaxLength: maxLength, Precision: precision, Scale: scale, IsNullable: isNullable, IsAutoInc: isIdentity, Type: colType})
	}
	if lastTable != "" {
		tables = append(tables, Table{Name: lastTable, Columns: columns})
	}
	for _, table := range tables {
		if err := writeTable(db, out, table.Name, table.Columns); err != nil {
			return err
		}
	}
	return nil
}

func writeTable(db *sql.DB, out io.Writer, table string, columns []Column) error {
	columnDefs := ""
	for i, column := range columns {
		if i > 0 {
			columnDefs += ",\n"
		}
		columnDefs += column.Name + " "
		if column.IsAutoInc {
			columnDefs += "serial"
		} else if column.Type == "varchar" || column.Type == "nvarchar" || column.Type == "text" || column.Type == "ntext" || (column.Type == "char" && column.MaxLength > 1) {
			if forceCaseInsensitive {
				columnDefs += "citext"
			} else {
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
		} else if column.Type == "datetime" {
			columnDefs += "timestamp"
		} else if column.Type == "image" {
			columnDefs += "bytea"
		} else if column.Type == "bit" {
			columnDefs += "boolean"
		} else {
			columnDefs += column.Type
		}
		if !column.IsNullable {
			columnDefs += " not"
		}
		columnDefs += " null"
	}
	fmt.Fprintf(out, "/* -- %s -- */\ndrop table if exists %s;\n\ncreate table %s\n(\n%s\n);\n\n", table, table, table, columnDefs)
	// Write data
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
				valStrs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
			case string:
				valStrs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
			default:
				valStrs[i] = fmt.Sprintf("%v", v)
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

func writeSeqReset(out io.Writer) {
	fmt.Fprintf(out, "-- function to find a sequence's field's maximum value, this is used to set the sequence's next value after the data is inserted\n")
	fmt.Fprintf(out, "-- see http://stackoverflow.com/a/5943183/1095458\n")
	fmt.Fprintf(out, `create or replace function seq_field_max_value(oid) returns bigint
volatile strict language plpgsql as $$ 
declare
 tabrelid oid;
 colname name;
 r record;
 newmax bigint;
beg
 for tabrelid, colname in select attrelid, attname
               from pg_attribute
              where (attrelid, attnum) in (
                      select adrelid::regclass,adnum
                        from pg_attrdef
                       where oid in (select objid
                                       from pg_depend
                                      where refobjid = $1
                                            and classid = 'pg_attrdef'::regclass
                                    )
          ) loop
      for r in execute 'select max(' || quote_ident(colname) || ') from ' || tabrelid::regclass loop
          if newmax is null or r.max > newmax then
              newmax := r.max;
          end if;
      end loop;
  end loop;
  return newmax;
end; $$ ;

`)
	fmt.Fprintf(out, "-- set any sequence to the maximum value of the sequence's field\n")
	fmt.Fprintf(out, "select relname, setval(oid, seq_field_max_value(oid)) from pg_class where relkind = 'S';\n\n")
}

func writeIndexes(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- PRIMARY KEYS, UNIQUE CONSTRAINTS AND INDEXES --------------------- */\n\n")

	rows, err := db.Query("select t.name as table_name, c.name as column_name, i.name as index_name, ic.index_column_id, i.is_primary_key, i.is_unique_constraint, i.is_unique from sys.indexes i, sys.index_columns ic, sys.tables t, sys.columns c where i.object_id = t.object_id and ic.object_id = t.object_id and ic.index_id = i.index_id and t.object_id = c.object_id and ic.column_id = c.column_id order by t.name asc, t.object_id asc, i.index_id asc, ic.index_column_id asc")
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

func writeForeignKeys(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- FOREIGN KEYS --------------------- */\n\n")

	rows, err := db.Query("select fk.name as key_name, t.name as parent_table, c.name as parent_column, rt.name as referenced_table, rc.name as referenced_column, fkc.constraint_column_id as constraint_column_id from sys.tables t, sys.tables rt, sys.columns c, sys.columns rc, sys.foreign_keys fk, sys.foreign_key_columns fkc where fk.object_id = fkc.constraint_object_id and t.object_id = fk.parent_object_id and fkc.parent_column_id = c.column_id and c.object_id = t.object_id and rt.object_id = fk.referenced_object_id and fkc.referenced_column_id = rc.column_id and rc.object_id = rt.object_id order by fk.object_id asc, fkc.constraint_column_id asc")
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

func writeDefaults(db *sql.DB, out io.Writer) error {
	fmt.Fprintf(out, "/* --------------------- DEFAULTS --------------------- */\n\n")

	rows, err := db.Query("select t.name as table_name, c.name as column_name, ty.name as type, d.name as default_name, d.definition from sys.tables t, sys.columns c, sys.types ty, sys.default_constraints d where t.object_id = c.object_id and c.user_type_id = ty.user_type_id and t.object_id = d.parent_object_id and c.column_id = d.parent_column_id order by t.name asc, t.object_id asc, c.column_id asc")
	if err != nil {
		return err
	}
	defer rows.Close()
	var lastTable string
	for rows.Next() {
		var tableName, columnName, colType, defaultName, definition string
		if err := rows.Scan(&tableName, &columnName, &colType, &defaultName, &definition); err != nil {
			return err
		}
		if lastTable != "" && lastTable != tableName {
			fmt.Fprintf(out, "\n")
		}
		lastTable = tableName
		def := definition
		def = strings.TrimPrefix(def, "((")
		def = strings.TrimSuffix(def, "))")
		def = strings.ReplaceAll(def, "(getdate())", "now()")
		if colType == "bit" {
			switch def {
			case "1":
				def = "true"
			case "0":
				def = "false"
			}
		}
		fmt.Fprintf(out, "alter table %s alter column %s set default %s;\n", tableName, columnName, def)
	}
	fmt.Fprintf(out, "\n")
	return nil
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
	var n int
	for rows.Next() {
		var text string
		var colid int
		if err := rows.Scan(&text, &colid); err != nil {
			return err
		}
		if colid == 1 {
			if n > 0 {
				fmt.Fprintf(out, ";\nGO\n\n")
			}
			n++
		}
		fmt.Fprintf(out, strings.TrimSpace(text))
	}
	if n > 0 {
		fmt.Fprintf(out, ";\nGO\n\n")
	}
	return nil
}

