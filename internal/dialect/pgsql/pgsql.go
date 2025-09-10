package pgsql

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"benritz/topgsql/internal/schema"
)

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
	}
	return dt
}

func fromDatatype(dt schema.DataType, textPref string) string {
	switch dt.Kind {
	case schema.KindSerialInt32:
		return "serial"
	case schema.KindSerialInt64:
		return "bigserial"
	case schema.KindInt16:
		return "smallint"
	case schema.KindInt32:
		return "integer"
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
		return "numeric(19, 4)"
	case schema.KindVarChar:
		if textPref == "text" {
			return "text"
		}
		if textPref == "citext" {
			return "citext"
		}
		if dt.Length <= 0 {
			return "text"
		}
		return fmt.Sprintf("varchar(%d)", dt.Length)
	case schema.KindText:
		if textPref == "citext" {
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
	case schema.KindUnknown:
		fallthrough
	default:
		if dt.Raw != "" {
			return dt.Raw
		}
		return "text"
	}
}

func GetTables(ctx context.Context, conn *pgx.Conn) ([]schema.Table, error) {
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

	tables := []schema.Table{}
	var lastTable string
	var columns []schema.Column

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
				tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
			}
			lastTable = tableName
			columns = []schema.Column{}
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

		columns = append(columns, schema.Column{
			ColumnID:   columnID,
			Name:       columnName,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
			IsComputed: isGenerated,
			IsAutoInc:  isAutoInc,
			Type:       baseType,
			Default:    *defaultValue,
			DataType:   dt,
		})
	}

	if lastTable != "" {
		tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
	}

	return tables, nil
}

func DropTableStatement(table schema.Table) string {
	return fmt.Sprintf("drop table if exists %s;", table.Name)
}

func CreateTableStatement(table schema.Table, textType string) string {
	columnDefs := ""

	for i, column := range table.Columns {
		if i > 0 {
			columnDefs += ",\n"
		}

		columnDefs += column.Name + " "
		columnDefs += fromDatatype(column.DataType, textType)

		if column.Default != "" && !column.IsAutoInc {
			columnDefs += " default " + column.Default
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
