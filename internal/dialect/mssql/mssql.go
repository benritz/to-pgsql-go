package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"benritz/topgsql/internal/schema"
	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlSource struct {
	db             *sql.DB
	tableCache     map[string]*schema.Table
	functionCache  []*schema.Function
	procedureCache []*schema.Procedure
	viewCache      []*schema.View
	triggerCache   []*schema.Trigger
}

func NewMssqlSource(connectionUrl string) (*MssqlSource, error) {
	db, err := sql.Open("sqlserver", connectionUrl)
	if err != nil {
		return nil, err
	}

	source := &MssqlSource{
		db: db,
	}

	return source, nil
}

func (s *MssqlSource) Close() {
	s.db.Close()
}

func (s *MssqlSource) GetTables(ctx context.Context) (map[string]*schema.Table, error) {
	if s.tableCache != nil {
		return s.tableCache, nil
	}

	tables, err := getTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.tableCache = make(map[string]*schema.Table, len(tables))
	for _, table := range tables {
		s.tableCache[strings.ToLower(table.Name)] = table
	}

	indexes, err := s.readIndexes(ctx, s.db, nil)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		if table, ok := s.tableCache[strings.ToLower(index.Table)]; ok {
			table.Indexes = append(table.Indexes, index)
			table.PKColNames = schema.PrimaryKeyColumns(table)
		}
	}

	keys, err := readForeignKeys(ctx, s.db, nil)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if table, ok := s.tableCache[strings.ToLower(key.Table)]; ok {
			table.ForeignKeyes = append(table.ForeignKeyes, key)
		}
	}

	return s.tableCache, nil
}

func (s *MssqlSource) GetTable(ctx context.Context, name string) (*schema.Table, error) {
	tablesMap, err := s.GetTables(ctx)
	if err != nil {
		return nil, err
	}
	if t, ok := tablesMap[strings.ToLower(name)]; ok {
		return t, nil
	}
	return nil, nil
}

func (s *MssqlSource) GetFunctions(ctx context.Context) ([]*schema.Function, error) {
	if s.functionCache != nil {
		return s.functionCache, nil
	}

	funcs, err := readFunctions(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.functionCache = funcs

	return s.functionCache, nil
}

func (s *MssqlSource) GetProcedures(ctx context.Context) ([]*schema.Procedure, error) {
	if s.procedureCache != nil {
		return s.procedureCache, nil
	}

	procs, err := readProcedures(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.procedureCache = procs

	return s.procedureCache, nil
}

func (s *MssqlSource) GetViews(ctx context.Context) ([]*schema.View, error) {
	if s.viewCache != nil {
		return s.viewCache, nil
	}

	views, err := readViews(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.viewCache = views

	return s.viewCache, nil
}

func (s *MssqlSource) GetTriggers(ctx context.Context) ([]*schema.Trigger, error) {
	if s.triggerCache != nil {
		return s.triggerCache, nil
	}

	triggers, err := readTriggers(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.triggerCache = triggers

	return s.triggerCache, nil
}

func (s *MssqlSource) NewTableDataReader(ctx context.Context) (*MssqlTableDataReader, error) {
	reader := MssqlTableDataReader{
		source: s,
	}

	return &reader, nil
}

type MssqlTableDataReader struct {
	source  *MssqlSource
	rows    *sql.Rows
	row     []any
	rowPtrs []any
}

func selectClause(cols []*schema.Column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = escapeIdentifier(c.Name)
	}
	return strings.Join(names, ", ")
}

func (r *MssqlTableDataReader) Open(ctx context.Context, table string, cols []*schema.Column) error {
	query := fmt.Sprintf("select %s from %s", selectClause(cols), table)
	rows, err := r.source.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	r.rows = rows
	r.row = make([]any, len(cols))
	r.rowPtrs = make([]any, len(cols))
	for i := range r.row {
		r.rowPtrs[i] = &r.row[i]
	}

	return nil
}

func (r *MssqlTableDataReader) ReadRow() ([]any, error) {
	if !r.rows.Next() {
		return []any{}, nil
	}
	err := r.rows.Scan(r.rowPtrs...)
	return r.row, err
}

func (r *MssqlTableDataReader) Close(ctx context.Context) error {
	defer r.rows.Close()
	if err := r.rows.Err(); err != nil {
		return err
	}
	return nil
}

func toDataType(baseType string, maxLength, precision, scale int, isAutoInc bool) schema.DataType {
	raw := strings.ToLower(baseType)
	dt := schema.DataType{Kind: schema.KindUnknown, Raw: raw}

	switch raw {
	case "bit":
		dt.Kind = schema.KindBool
	case "tinyint":
		dt.Kind = schema.KindInt16
	case "smallint":
		dt.Kind = schema.KindInt16
	case "int":
		if isAutoInc {
			dt.Kind = schema.KindSerialInt32
		} else {
			dt.Kind = schema.KindInt32
		}
	case "bigint":
		if isAutoInc {
			dt.Kind = schema.KindSerialInt64
		} else {
			dt.Kind = schema.KindInt64
		}
	case "real":
		dt.Kind = schema.KindFloat32
	case "float":
		dt.Kind = schema.KindFloat64
	case "decimal", "numeric":
		dt.Kind = schema.KindNumeric
		dt.Precision = precision
		dt.Scale = scale
	case "money", "smallmoney":
		dt.Kind = schema.KindMoney
	case "uniqueidentifier":
		dt.Kind = schema.KindUUID
	case "varchar", "nvarchar", "char", "nchar":
		unbounded := maxLength == -1
		length := maxLength
		if raw == "nvarchar" || raw == "nchar" {
			if length > 0 {
				length /= 2
			}
		}
		if unbounded {
			dt.Kind = schema.KindText
		} else {
			dt.Kind = schema.KindVarChar
			dt.Length = length
		}
	case "text", "ntext":
		dt.Kind = schema.KindText
	case "varbinary", "binary", "image", "rowversion", "timestamp":
		dt.Kind = schema.KindBinary
		if maxLength > 0 && maxLength != -1 {
			dt.Length = maxLength
		}
	case "date":
		dt.Kind = schema.KindDate
	case "time":
		dt.Kind = schema.KindTime
	case "datetime", "smalldatetime", "datetime2":
		dt.Kind = schema.KindTimestamp
	case "datetimeoffset":
		dt.Kind = schema.KindTimestamp
		dt.Timezone = true
	}

	return dt
}

func fromDataType(dt schema.DataType) string {
	switch dt.Kind {
	case schema.KindSerialInt32, schema.KindInt32:
		return "int"
	case schema.KindSerialInt64, schema.KindInt64:
		return "bigint"
	case schema.KindInt16:
		return "smallint"
	case schema.KindBool:
		return "bit"
	case schema.KindUUID:
		return "uniqueidentifier"
	case schema.KindFloat32:
		return "real"
	case schema.KindFloat64:
		return "float"
	case schema.KindNumeric:
		if dt.Precision > 0 {
			return fmt.Sprintf("decimal(%d,%d)", dt.Precision, dt.Scale)
		}
		return "decimal"
	case schema.KindMoney:
		return "money"
	case schema.KindVarChar:
		if dt.Length > 0 {
			return fmt.Sprintf("varchar(%d)", dt.Length)
		}
		return "varchar(max)"
	case schema.KindText:
		return "varchar(max)"
	case schema.KindBinary:
		if dt.Length > 0 {
			return fmt.Sprintf("varbinary(%d)", dt.Length)
		}
		return "varbinary(max)"
	case schema.KindDate:
		return "date"
	case schema.KindTime:
		return "time"
	case schema.KindTimestamp:
		if dt.Timezone {
			return "datetimeoffset"
		}
		return "datetime2"
	case schema.KindUnknown:
		fallthrough
	default:
		return "varchar(max)"
	}
}

func getTables(ctx context.Context, db *sql.DB) ([]*schema.Table, error) {
	rows, err := db.QueryContext(
		ctx,
		`select 
 t.name as table_name, 
 c.column_id, 
 c.name as column_name, 
 c.max_length, 
 c.precision, 
 c.scale, 
 c.is_nullable, 
 c.is_identity, 
 c.is_computed,
 ty.name as type, 
 d.definition as default_value
 from 
 sys.tables t join sys.columns c on t.object_id = c.object_id 
 join sys.types ty on c.user_type_id = ty.user_type_id
 left join sys.default_constraints d on d.parent_object_id = c.object_id and d.parent_column_id = c.column_id
 order by 
 t.name asc, t.object_id asc, c.column_id asc`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*schema.Table{}
	var lastTable string
	var columns []*schema.Column

	for rows.Next() {
		var tableName, columnName, colType, defaultValue string
		var columnID, maxLength, precision, scale int
		var isNullable, isComputed, isAutoInc bool
		var defaultValueOrNull sql.NullString

		if err := rows.Scan(
			&tableName,
			&columnID,
			&columnName,
			&maxLength,
			&precision,
			&scale,
			&isNullable,
			&isAutoInc,
			&isComputed,
			&colType,
			&defaultValueOrNull,
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

		dt := toDataType(colType, maxLength, precision, scale, isAutoInc)

		defaultValue = ""
		if defaultValueOrNull.Valid {
			defaultValue = translateDefault(dt, defaultValueOrNull.String)

			if !isAutoInc &&
				strings.HasPrefix(strings.ToLower(strings.TrimSpace(defaultValue)), "next value for ") {
				isAutoInc = true
				// recompute data type for auto-increment
				dt = toDataType(colType, maxLength, precision, scale, isAutoInc)
			}
		}

		columns = append(columns, &schema.Column{
			ColumnID:   columnID,
			Name:       columnName,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
			IsComputed: isComputed,
			IsAutoInc:  isAutoInc,
			Default:    defaultValue,
			DataType:   dt,
		})
	}

	if rows.Err() != nil {
		return nil, err
	}

	if lastTable != "" {
		tables = append(tables, &schema.Table{Name: lastTable, Columns: columns})
	}

	return tables, nil
}

func stripEnclosing(s, first, last string) string {
	s = strings.TrimSpace(s)
	if len(s) >= len(first)+len(last) &&
		strings.HasPrefix(s, first) &&
		strings.HasSuffix(s, last) {
		return stripEnclosing(s[len(first):len(s)-len(last)], first, last)
	}
	return s
}

func translateDefault(dt schema.DataType, defaultValue string) string {
	defaultValue = strings.TrimSpace(defaultValue)
	if defaultValue == "" {
		return ""
	}

	v := stripEnclosing(defaultValue, "(", ")")
	v = translateNowFn(v)

	if dt.Kind == schema.KindBool {
		v = stripEnclosing(v, "'", "'")
		switch v {
		case "1", "-1":
			v = "{{True}}"
		case "0":
			v = "{{False}}"
		}
	}

	return v
}

func translateNowFn(s string) string {
	replacements := []struct{ pattern, replacement string }{
		{`(?i)getdate\(\)`, "{{Now.Local}}"},
		{`(?i)sysdatetime\(\)`, "{{Now.Local}}"},
		{`(?i)getutcdate\(\)`, "{{Now.UTC}}"},
		{`(?i)sysutcdatetime\(\)`, "{{Now.UTC}}"},
	}
	for _, r := range replacements {
		re := regexp.MustCompile(r.pattern)
		s = re.ReplaceAllString(s, r.replacement)
	}
	return s
}

func translateQuotedIdentifier(s string) string {
	re := regexp.MustCompile(`\[([^\]]+)\]`)
	return re.ReplaceAllString(s, "$1")
}

func translateCoalesceFn(s string) string {
	re := regexp.MustCompile(`(?i)isnull\s*\(`)
	return re.ReplaceAllString(s, "coalesce(")
}

func translateBool(s string, table *schema.Table) string {
	if table == nil {
		return s
	}

	re := regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\b\s*=\s*\('?(-?1|0)'?\)?`)

	s = re.ReplaceAllStringFunc(s, func(m string) string {
		sub := re.FindStringSubmatch(m)
		if len(sub) != 3 {
			return m
		}
		columnName := sub[1]
		value := sub[2]

		col := table.GetColumn(columnName)
		if col == nil {
			return m
		}

		if col.DataType.Kind != schema.KindBool {
			return m
		}

		var marker string
		switch value {
		case "1", "-1":
			marker = "{{True}}"
		case "0":
			marker = "{{False}}"
		default:
			return m
		}

		return fmt.Sprintf("%s=%s", columnName, marker)
	})

	return s
}

func translateDatePartFn(s string) string {
	re := regexp.MustCompile(`(?i)datepart\s*\(([^,]+),\s*([^)]+)\)`)
	return re.ReplaceAllString(s, "extract($1 from $2)")
}

func translateIndexFilter(filter string, table *schema.Table) string {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return ""
	}

	filter = stripEnclosing(filter, "(", ")")
	filter = translateNowFn(filter)
	filter = translateQuotedIdentifier(filter)
	filter = translateCoalesceFn(filter)
	filter = translateBool(filter, table)

	return filter
}

func escapeIdentifier(identifier string) string {
	return fmt.Sprintf("[%s]", identifier)
}

func (s *MssqlSource) readIndexes(ctx context.Context, db *sql.DB, table *schema.Table) ([]*schema.Index, error) {
	q := `SELECT 
    t.name AS table_name,
    i.name AS index_name,
    c.name AS column_name,
    ic.index_column_id,
    ic.key_ordinal,
    ic.is_included_column,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_unique,
    i.has_filter,
    i.filter_definition
	FROM sys.indexes i
	JOIN sys.tables t ON i.object_id = t.object_id
	JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id`
	args := []any{}

	if table != nil {
		q += " WHERE t.name = @p1 "
		args = append(args, table.Name)
	}

	q += " ORDER BY t.name ASC, t.object_id ASC, i.index_id ASC, ic.index_column_id ASC"

	rows, err := db.QueryContext(ctx, q, args...)
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
			indexColumnID, keyOrdinal        int
			isIncluded                       bool
			pk, uc, uq                       bool
			hasFilter                        bool
			filterDefinitionNull             sql.NullString
		)

		if err := rows.Scan(
			&tableName,
			&indexName,
			&columnName,
			&indexColumnID,
			&keyOrdinal,
			&isIncluded,
			&pk,
			&uc,
			&uq,
			&hasFilter,
			&filterDefinitionNull,
		); err != nil {
			return nil, err
		}

		if indexName != lastIndex || tableName != lastTable {
			if cur != nil {
				result = append(result, cur)
			}

			var it schema.IndexType
			if pk {
				it = schema.IndexTypePrimaryKey
			} else if uc {
				it = schema.IndexTypeUniqueConstraint
			} else if uq {
				it = schema.IndexTypeUnique
			} else {
				it = schema.IndexTypeNonUnique
			}

			filter := ""
			if hasFilter && filterDefinitionNull.Valid {
				table, err := s.GetTable(ctx, tableName)
				if err != nil {
					return nil, err
				}
				filter = translateIndexFilter(filterDefinitionNull.String, table)
			}

			cur = &schema.Index{
				Table:          tableName,
				Name:           indexName,
				Columns:        []string{},
				IncludeColumns: []string{},
				Filter:         filter,
				IndexType:      it,
			}

			lastTable = tableName
			lastIndex = indexName
		}

		// keyOrdinal > 0 indicates key column order; 0 means included column
		if isIncluded || keyOrdinal == 0 {
			cur.IncludeColumns = append(cur.IncludeColumns, columnName)
		} else {
			cur.Columns = append(cur.Columns, columnName)
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

func readForeignKeys(ctx context.Context, db *sql.DB, table *schema.Table) ([]*schema.ForeignKey, error) {
	q := `SELECT
	    fk.name AS key_name,
	    t.name AS parent_table,
	    c.name AS parent_column,
	    rt.name AS referenced_table,
	    rc.name AS referenced_column,
	    fkc.constraint_column_id AS constraint_column_id
	FROM sys.foreign_keys fk
	JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
	JOIN sys.tables t ON t.object_id = fk.parent_object_id AND t.object_id = fkc.parent_object_id
	JOIN sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
	JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id AND rt.object_id = fkc.referenced_object_id
	JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
	WHERE fk.is_ms_shipped = 0`
	args := []any{}

	if table != nil {
		q += " and t.name = @p1"
		args = append(args, table.Name)
	}

	q += " order by fk.object_id asc, fkc.constraint_column_id asc"

	rows, err := db.QueryContext(ctx, q, args...)
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
			constraintColumnID                        int
		)
		if err := rows.Scan(&keyName, &pTable, &pColumn, &rTable, &rColumn, &constraintColumnID); err != nil {
			return nil, err
		}

		if constraintColumnID == 1 {
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

func readCompiledObjects[T any](ctx context.Context, db *sql.DB, objType string, builder func(name, definition string) T) ([]T, error) {
	query := `select object_schema_name(o.object_id) as schema_name, o.name, object_definition(o.object_id) as definition
		from sys.objects o
		where o.type = @p1 and o.is_ms_shipped = 0
		order by object_schema_name(o.object_id), o.name`

	rows, err := db.QueryContext(ctx, query, objType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	for rows.Next() {
		var schemaName, name string
		var def sql.NullString
		if err := rows.Scan(&schemaName, &name, &def); err != nil {
			return nil, err
		}
		if !def.Valid {
			continue
		}
		// qualified := fmt.Sprintf("%s.%s", schemaName, name)
		definition := normalizeLineEndings(def.String)
		if objType == "V" {
			definition = translateViewDef(definition)
		}
		result = append(result, builder(name, definition))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func readFunctions(ctx context.Context, db *sql.DB) ([]*schema.Function, error) {
	return readCompiledObjects(ctx, db, "FN", func(name, definition string) *schema.Function {
		return &schema.Function{Name: name, Definition: definition}
	})
}

func readProcedures(ctx context.Context, db *sql.DB) ([]*schema.Procedure, error) {
	return readCompiledObjects(ctx, db, "P", func(name, definition string) *schema.Procedure {
		return &schema.Procedure{Name: name, Definition: definition}
	})
}

func readViews(ctx context.Context, db *sql.DB) ([]*schema.View, error) {
	return readCompiledObjects(ctx, db, "V", func(name, definition string) *schema.View {
		return &schema.View{Name: name, Definition: definition}
	})
}

func readTriggers(ctx context.Context, db *sql.DB) ([]*schema.Trigger, error) {
	return readCompiledObjects(ctx, db, "TR", func(name, definition string) *schema.Trigger {
		return &schema.Trigger{Name: name, Definition: definition}
	})
}

func normalizeLineEndings(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func translateViewDef(s string) string {
	s = strings.TrimSpace(s)

	s = translateQuotedIdentifier(s)
	s = translateDatePartFn(s)

	// reNLiteral := regexp.MustCompile(`(?i)\bN'((?:[^']|'')*)'`)
	// s = reNLiteral.ReplaceAllString(s, `'${1}'`)

	reAsSingleAlias := regexp.MustCompile(`(?i)\bas\s*'([^']+)'`)
	s = reAsSingleAlias.ReplaceAllString(s, "AS $1")
	reAsDoubleAlias := regexp.MustCompile(`(?i)\bas\s*"([^"]+)"`)
	s = reAsDoubleAlias.ReplaceAllString(s, "AS $1")

	reConcatLeft := regexp.MustCompile(`(?i)(N?'(?:[^']|'')*')\s*\+\s*`)
	s = reConcatLeft.ReplaceAllString(s, "$1 || ")
	reConcatRight := regexp.MustCompile(`(?i)\s*\+\s*(N?'(?:[^']|'')*')`)
	s = reConcatRight.ReplaceAllString(s, " || $1")

	reTrailingGo := regexp.MustCompile(`(?mi)(?:\r?\n)[ \t]*GO[ \t]*\s*$`)
	s = reTrailingGo.ReplaceAllString(s, "\n")

	if !strings.HasSuffix(s, ";") {
		s += ";"
	}

	return s
}
