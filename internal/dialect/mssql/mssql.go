package mssql

import (
	"database/sql"
	"strings"

	"benritz/topgsql/internal/schema"
)

func GetTables(db *sql.DB) ([]schema.Table, error) {
	rows, err := db.Query(
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
d.definition
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

		defaultValue = ""
		if defaultValueOrNull.Valid {
			defaultValue = defaultValueOrNull.String

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
		})
	}

	if lastTable != "" {
		tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
	}

	return tables, nil
}
