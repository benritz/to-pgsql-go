package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"benritz/topgsql/internal/schema"
	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlSource struct {
	db              *sql.DB
	tableCache      map[string]schema.Table
	indexCache      []schema.Index
	foreignKeyCache []schema.ForeignKey
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

func (s *MssqlSource) GetTables(ctx context.Context) (map[string]schema.Table, error) {
	if s.tableCache != nil {
		return s.tableCache, nil
	}

	tables, err := getTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	s.tableCache = make(map[string]schema.Table, len(tables))
	for _, table := range tables {
		s.tableCache[strings.ToLower(table.Name)] = table
	}

	return s.tableCache, nil
}

func (s *MssqlSource) GetIndexes(ctx context.Context) ([]schema.Index, error) {
	if s.indexCache != nil {
		return s.indexCache, nil
	}

	indexes, err := readIndexes(ctx, s.db, nil)
	if err != nil {
		return nil, err
	}

	s.indexCache = indexes

	return s.indexCache, nil
}

func (s *MssqlSource) GetForeignKeys(ctx context.Context) ([]schema.ForeignKey, error) {
	if s.foreignKeyCache != nil {
		return s.foreignKeyCache, nil
	}

	keys, err := readForeignKeys(ctx, s.db, nil)
	if err != nil {
		return nil, err
	}

	s.foreignKeyCache = keys

	return s.foreignKeyCache, nil
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

func selectClause(cols []schema.Column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return strings.Join(names, ", ")
}

func (r *MssqlTableDataReader) Open(ctx context.Context, table string, cols []schema.Column) error {
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

func fromDatatype(dt schema.DataType) string {
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
		if dt.Raw != "" {
			return dt.Raw
		}
		return "varchar(max)"
	}
}

func getTables(ctx context.Context, db *sql.DB) ([]schema.Table, error) {
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

	tables := []schema.Table{}
	var lastTable string
	var columns []schema.Column

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
				tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
			}
			lastTable = tableName
			columns = []schema.Column{}
		}

		dt := toDataType(colType, maxLength, precision, scale, isAutoInc)

		defaultValue = ""
		if defaultValueOrNull.Valid {
			defaultValue = translateDefault(dt, defaultValueOrNull.String)

			if !isAutoInc &&
				strings.HasPrefix(strings.ToLower(strings.TrimSpace(defaultValue)), "next value for ") {
				isAutoInc = true
			}
		}

		columns = append(columns, schema.Column{
			ColumnID:   columnID,
			Name:       columnName,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
			IsComputed: isComputed,
			IsAutoInc:  isAutoInc,
			Type:       colType,
			Default:    defaultValue,
			DataType:   dt,
		})
	}

	if rows.Err() != nil {
		return nil, err
	}

	if lastTable != "" {
		tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
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
	if defaultValue == "" {
		return ""
	}

	v := stripEnclosing(defaultValue, "(", ")")

	if strings.EqualFold(v, "getutcdate()") ||
		strings.EqualFold(v, "sysutcdatetime()") {
		v = "{{Now.UTC}}"
	} else if strings.EqualFold(v, "getdate()") ||
		strings.EqualFold(v, "sysdatetime()") ||
		strings.EqualFold(v, "current_timestamp") {
		v = "{{Now.Local}}"
	}

	if dt.Kind == schema.KindBool {
		v = stripEnclosing(v, "'", "'")
		switch v {
		case "1", "-1":
			v = "true"
		case "0":
			v = "false"
		}
	}

	return v
}

func readIndexes(ctx context.Context, db *sql.DB, table *schema.Table) ([]schema.Index, error) {
	q := "select t.name as table_name, c.name as column_name, i.name as index_name, ic.index_column_id, i.is_primary_key, i.is_unique_constraint, i.is_unique from sys.indexes i, sys.index_columns ic, sys.tables t, sys.columns c where i.object_id = t.object_id and ic.object_id = t.object_id and ic.index_id = i.index_id and t.object_id = c.object_id and ic.column_id = c.column_id"
	args := []any{}

	if table != nil {
		q += " and t.name = @p1 "
		args = append(args, table.Name)
	}

	q += " order by t.name asc, t.object_id asc, i.index_id asc, ic.index_column_id asc"

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		result []schema.Index
		cur    schema.Index
	)

	for rows.Next() {
		var (
			tableName, columnName, indexName string
			indexColumnID                    int
			pk, uc, uq                       bool
		)

		if err := rows.Scan(&tableName, &columnName, &indexName, &indexColumnID, &pk, &uc, &uq); err != nil {
			return nil, err
		}

		if indexColumnID == 1 {
			if cur.Name != "" {
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

			cur = schema.Index{
				Table:     tableName,
				Name:      indexName,
				Columns:   []string{},
				IndexType: it,
			}
		}

		cur.Columns = append(cur.Columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if cur.Name != "" {
		result = append(result, cur)
	}

	return result, nil
}

func readForeignKeys(ctx context.Context, db *sql.DB, table *schema.Table) ([]schema.ForeignKey, error) {
	q := "select fk.name as key_name, t.name as parent_table, c.name as parent_column, rt.name as referenced_table, rc.name as referenced_column, fkc.constraint_column_id as constraint_column_id from sys.tables t, sys.tables rt, sys.columns c, sys.columns rc, sys.foreign_keys fk, sys.foreign_key_columns fkc where fk.object_id = fkc.constraint_object_id and t.object_id = fk.parent_object_id and fkc.parent_column_id = c.column_id and c.object_id = t.object_id and rt.object_id = fk.referenced_object_id and fkc.referenced_column_id = rc.column_id and rc.object_id = rt.object_id"
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
		result                    []schema.ForeignKey
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
				result = append(result, schema.ForeignKey{
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
		result = append(result, schema.ForeignKey{
			Name:              curKey,
			Table:             curParent,
			Columns:           curParentCols,
			ReferencedTable:   curRef,
			ReferencedColumns: curRefCols,
		})
	}

	return result, nil
}
