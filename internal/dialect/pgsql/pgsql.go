package pgsql

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"benritz/topgsql/internal/schema"
)

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
		})
	}

	if lastTable != "" {
		tables = append(tables, schema.Table{Name: lastTable, Columns: columns})
	}

	return tables, nil
}
