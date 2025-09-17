package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"benritz/topgsql/internal/schema"

	"gopkg.in/yaml.v3"
)

type Root struct {
	Source          SourceSection  `yaml:"source"`
	Target          TargetSection  `yaml:"target"`
	Include         IncludeSection `yaml:"include"`
	Schema          SchemaSection  `yaml:"schema"`
	Scripts         []string       `yaml:"scripts"`
	ScriptsBasePath string         `yaml:"scripts_base_path"`
}

type SourceSection struct {
	URL string `yaml:"url"`
}

type TargetSection struct {
	URL           string `yaml:"url"`
	TextType      string `yaml:"text_type"`
	DataBatchSize int    `yaml:"data_batch_size"`
}

type IncludeSection struct {
	Data       bool `yaml:"data"`
	Tables     bool `yaml:"tables"`
	Functions  bool `yaml:"functions"`
	Triggers   bool `yaml:"triggers"`
	Procedures bool `yaml:"procedures"`
	Views      bool `yaml:"views"`
	Scripts    bool `yaml:"scripts"`
}

type SchemaSection struct {
	Tables []TableDef `yaml:"tables"`
}

type TableDef struct {
	Name        string          `yaml:"name"`
	Strategy    string          `yaml:"strategy"`
	Comment     string          `yaml:"comment"`
	Columns     []ColumnDef     `yaml:"columns"`
	Indexes     []IndexDef      `yaml:"indexes"`
	ForeignKeys []ForeignKeyDef `yaml:"foreign_keys"`
}

type ColumnDef struct {
	Name          string `yaml:"name"`
	Kind          string `yaml:"kind"`
	Length        *int   `yaml:"length"`
	Precision     *int   `yaml:"precision"`
	Scale         *int   `yaml:"scale"`
	Timezone      *bool  `yaml:"timezone"`
	Nullable      *bool  `yaml:"nullable"`
	AutoIncrement *bool  `yaml:"auto_increment"`
	Computed      *bool  `yaml:"computed"`
	Default       string `yaml:"default"`
}

type IndexDef struct {
	Name           string   `yaml:"name"`
	Type           string   `yaml:"type"`
	Columns        []string `yaml:"columns"`
	IncludeColumns []string `yaml:"include_columns"`
	Filter         string   `yaml:"filter"`
}

type ForeignKeyDef struct {
	Name              string   `yaml:"name"`
	Columns           []string `yaml:"columns"`
	ReferencedTable   string   `yaml:"referenced_table"`
	ReferencedColumns []string `yaml:"referenced_columns"`
}

func LoadFile(path string) (*Root, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	root, err := Load(f)
	if err != nil {
		return nil, err
	}

	if root.ScriptsBasePath == "" {
		root.ScriptsBasePath = filepath.Dir(path)
	}

	return root, nil
}

func Load(r io.Reader) (*Root, error) {
	var rs io.ReadSeeker
	if seeker, ok := r.(io.ReadSeeker); ok {
		rs = seeker
	} else {
		buf, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		rs = bytes.NewReader(buf)
	}

	if err := validateReader(rs); err != nil {
		return nil, err
	}

	var cfg Root
	dec := yaml.NewDecoder(rs)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	expandEnv(&cfg)

	return &cfg, nil
}

func expandEnv(cfg *Root) {
	cfg.Source.URL = os.ExpandEnv(cfg.Source.URL)
	cfg.Target.URL = os.ExpandEnv(cfg.Target.URL)
	cfg.ScriptsBasePath = os.ExpandEnv(cfg.ScriptsBasePath)
}

func BuildTables(tableDefs []TableDef, tablesMap map[string]*schema.Table) (map[string]*schema.Table, error) {
	if tablesMap == nil {
		tablesMap = map[string]*schema.Table{}
	}

	out := make(map[string]*schema.Table, len(tablesMap))
	for _, v := range tablesMap {
		out[strings.ToLower(v.Name)] = v
	}

	for _, td := range tableDefs {
		key := strings.ToLower(td.Name)

		strategy := td.Strategy
		if strategy == "" {
			strategy = "replace"
		}

		cur, ok := out[key]

		var table *schema.Table
		var err error

		if strategy == "replace" || !ok {
			table, err = configTableToSchema(td)
		} else {
			table, err = mergeTable(cur, &td)
		}
		if err != nil {
			return nil, err
		}
		out[key] = table

		for _, id := range td.Indexes {
			index, err := buildIndex(id, td.Name)
			if err != nil {
				return nil, err
			}
			table.Indexes = append(table.Indexes, index)
		}

		for _, fkd := range td.ForeignKeys {
			fk, err := buildForeignKey(fkd, td.Name)
			if err != nil {
				return nil, err
			}
			table.ForeignKeyes = append(table.ForeignKeyes, fk)
		}
	}

	return out, nil
}

func configTableToSchema(td TableDef) (*schema.Table, error) {
	cols := make([]*schema.Column, 0, len(td.Columns))
	for i, c := range td.Columns {
		col, err := buildColumn(c, i+1)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return &schema.Table{Name: td.Name, Columns: cols}, nil
}

func mergeTable(base *schema.Table, td *TableDef) (*schema.Table, error) {
	idxMap := map[string]int{}
	for i, c := range base.Columns {
		idxMap[strings.ToLower(c.Name)] = i
	}
	for _, cdef := range td.Columns {
		key := strings.ToLower(cdef.Name)
		if i, ok := idxMap[key]; ok {
			col := base.Columns[i]
			if err := overrideColumn(col, cdef); err != nil {
				return base, err
			}
		} else {
			col, err := buildColumn(cdef, len(base.Columns)+1)
			if err != nil {
				return base, err
			}
			base.Columns = append(base.Columns, col)
		}
	}
	return base, nil
}

func buildColumn(c ColumnDef, ordinal int) (*schema.Column, error) {
	dt, err := buildDataType(c)
	if err != nil {
		return nil, err
	}

	nullable := true
	if c.Nullable != nil {
		nullable = *c.Nullable
	}

	autoIncrement := false
	if c.AutoIncrement != nil {
		autoIncrement = *c.AutoIncrement
	}

	computed := false
	if c.Computed != nil {
		computed = *c.Computed
	}

	return &schema.Column{
		ColumnID:   ordinal,
		Name:       c.Name,
		DataType:   dt,
		MaxLength:  intVal(c.Length),
		Precision:  intVal(c.Precision),
		Scale:      intVal(c.Scale),
		IsNullable: nullable,
		IsComputed: computed,
		IsAutoInc:  autoIncrement,
		Default:    c.Default,
	}, nil
}

func overrideColumn(col *schema.Column, colDef ColumnDef) error {
	if colDef.Kind != "" {
		dt, err := buildDataType(colDef)
		if err != nil {
			return err
		}
		col.DataType = dt
	}

	if colDef.Length != nil {
		col.MaxLength = *colDef.Length
		col.DataType.Length = *colDef.Length
	}

	if colDef.Precision != nil {
		col.Precision = *colDef.Precision
		col.DataType.Precision = *colDef.Precision
	}

	if colDef.Scale != nil {
		col.Scale = *colDef.Scale
		col.DataType.Scale = *colDef.Scale
	}

	if colDef.Timezone != nil {
		col.DataType.Timezone = *colDef.Timezone
	}

	if colDef.Nullable != nil {
		col.IsNullable = *colDef.Nullable
	}

	if colDef.AutoIncrement != nil {
		col.IsAutoInc = *colDef.AutoIncrement
	}

	if colDef.Computed != nil {
		col.IsComputed = *colDef.Computed
	}

	if colDef.Default != "" {
		col.Default = colDef.Default
	}

	return nil
}

func buildDataType(colDef ColumnDef) (schema.DataType, error) {
	var dt schema.DataType

	kind := strings.ToLower(colDef.Kind)
	switch kind {
	case "bool":
		dt.Kind = schema.KindBool
	case "int16":
		dt.Kind = schema.KindInt16
	case "int32":
		if boolVal(colDef.AutoIncrement) {
			dt.Kind = schema.KindSerialInt32
		} else {
			dt.Kind = schema.KindInt32
		}
	case "int64":
		if boolVal(colDef.AutoIncrement) {
			dt.Kind = schema.KindSerialInt64
		} else {
			dt.Kind = schema.KindInt64
		}
	case "serial_int32":
		dt.Kind = schema.KindSerialInt32
	case "serial_int64":
		dt.Kind = schema.KindSerialInt64
	case "float32":
		dt.Kind = schema.KindFloat32
	case "float64":
		dt.Kind = schema.KindFloat64
	case "numeric":
		dt.Kind = schema.KindNumeric
	case "money":
		dt.Kind = schema.KindMoney
	case "uuid":
		dt.Kind = schema.KindUUID
	case "varchar":
		dt.Kind = schema.KindVarChar
	case "text":
		dt.Kind = schema.KindText
	case "binary":
		dt.Kind = schema.KindBinary
	case "date":
		dt.Kind = schema.KindDate
	case "time":
		dt.Kind = schema.KindTime
	case "timestamp":
		dt.Kind = schema.KindTimestamp
	case "json":
		dt.Kind = schema.KindJson
	default:
		return schema.DataType{}, fmt.Errorf("unknown column kind: %s", colDef.Kind)
	}

	if colDef.Length != nil {
		dt.Length = *colDef.Length
	}

	if colDef.Precision != nil {
		dt.Precision = *colDef.Precision
	}

	if colDef.Scale != nil {
		dt.Scale = *colDef.Scale
	}

	if colDef.Timezone != nil {
		dt.Timezone = *colDef.Timezone
	}

	return dt, nil
}

func buildIndex(def IndexDef, tableName string) (*schema.Index, error) {
	indexType, err := parseIndexType(def.Type)
	if err != nil {
		return nil, err
	}
	return &schema.Index{
		Table:          tableName,
		Name:           def.Name,
		Columns:        def.Columns,
		IncludeColumns: def.IncludeColumns,
		Filter:         def.Filter,
		IndexType:      indexType,
	}, nil
}

func buildForeignKey(def ForeignKeyDef, tableName string) (*schema.ForeignKey, error) {
	return &schema.ForeignKey{
		Name:              def.Name,
		Table:             tableName,
		Columns:           def.Columns,
		ReferencedTable:   def.ReferencedTable,
		ReferencedColumns: def.ReferencedColumns,
	}, nil
}

func parseIndexType(s string) (schema.IndexType, error) {
	switch strings.ToLower(s) {
	case "primary_key":
		return schema.IndexTypePrimaryKey, nil
	case "unique_constraint":
		return schema.IndexTypeUniqueConstraint, nil
	case "unique":
		return schema.IndexTypeUnique, nil
	case "non_unique":
		return schema.IndexTypeNonUnique, nil
	default:
		return "", fmt.Errorf("invalid index type: %s", s)
	}
}

func intVal(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func boolVal(p *bool) bool {
	if p == nil {
		return false
	}
	return *p
}
