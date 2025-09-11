package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"benritz/topgsql/internal/schema"
)

type MssqlSource struct {
	db         *sql.DB
	tableCache map[string]schema.Table
}

func NewMssqlTarget(connectionUrl string) (*MssqlSource, error) {
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
		s.tableCache[table.Name] = table
	}

	return s.tableCache, nil
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

		dt := toDataType(colType, maxLength, precision, scale, isAutoInc)

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
