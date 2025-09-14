package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"benritz/topgsql/internal/migrate"
	"benritz/topgsql/internal/schema"

	"gopkg.in/yaml.v3"
)

type Root struct {
	Source  SourceSection  `yaml:"source"`
	Target  TargetSection  `yaml:"target"`
	Include IncludeSection `yaml:"include"`
	Schema  SchemaSection  `yaml:"schema"`
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
	PrimaryKey    *bool  `yaml:"primary_key"`
	Unique        *bool  `yaml:"unique"`
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
	return Load(f)
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
}

func (c *Root) BuildTables(introspected map[string]schema.Table) ([]schema.Table, []schema.Index, []schema.ForeignKey, error) {
	if introspected == nil {
		introspected = map[string]schema.Table{}
	}
	out := make(map[string]schema.Table, len(introspected))
	for _, v := range introspected {
		out[strings.ToLower(v.Name)] = v
	}

	var idxs []schema.Index
	var fks []schema.ForeignKey

	for _, td := range c.Schema.Tables {
		key := strings.ToLower(td.Name)
		strategy := td.Strategy
		if strategy == "" {
			strategy = "merge"
		}
		if strategy == "replace" {
			ct, err := configTableToSchema(td)
			if err != nil {
				return nil, nil, nil, err
			}
			out[key] = ct
		} else {
			cur, ok := out[key]
			if !ok {
				cur = schema.Table{Name: td.Name}
			}
			merged, err := mergeTable(cur, td)
			if err != nil {
				return nil, nil, nil, err
			}
			out[key] = merged
		}

		for _, id := range td.Indexes {
			index, err := buildIndex(id, td.Name)
			if err != nil {
				return nil, nil, nil, err
			}
			idxs = append(idxs, index)
		}
		for _, fk := range td.ForeignKeys {
			fkObj, err := buildForeignKey(fk, td.Name)
			if err != nil {
				return nil, nil, nil, err
			}
			fks = append(fks, fkObj)
		}
		for _, cd := range td.Columns {
			if cd.PrimaryKey != nil && *cd.PrimaryKey {
				idxs = append(idxs, schema.Index{
					Table:     td.Name,
					Name:      fmt.Sprintf("%s_%s_pkey", td.Name, cd.Name),
					Columns:   []string{cd.Name},
					IndexType: schema.IndexTypePrimaryKey,
				})
			} else if cd.Unique != nil && *cd.Unique {
				idxs = append(idxs, schema.Index{
					Table:     td.Name,
					Name:      fmt.Sprintf("%s_%s_uq", td.Name, cd.Name),
					Columns:   []string{cd.Name},
					IndexType: schema.IndexTypeUnique,
				})
			}
		}
	}

	// Validate index & FK column existence
	for _, ix := range idxs {
		tbl, ok := out[strings.ToLower(ix.Table)]
		if !ok {
			return nil, nil, nil, fmt.Errorf("index %s references missing table %s", ix.Name, ix.Table)
		}
		colset := map[string]struct{}{}
		for _, c := range tbl.Columns {
			colset[strings.ToLower(c.Name)] = struct{}{}
		}
		for _, c := range append(ix.Columns, ix.IncludeColumns...) {
			if _, ok := colset[strings.ToLower(c)]; !ok {
				return nil, nil, nil, fmt.Errorf("index %s column %s not in table %s", ix.Name, c, ix.Table)
			}
		}
	}
	for _, fk := range fks {
		tbl, ok := out[strings.ToLower(fk.Table)]
		if !ok {
			return nil, nil, nil, fmt.Errorf("foreign key %s missing table %s", fk.Name, fk.Table)
		}
		colset := map[string]struct{}{}
		for _, c := range tbl.Columns {
			colset[strings.ToLower(c.Name)] = struct{}{}
		}
		for _, c := range fk.Columns {
			if _, ok := colset[strings.ToLower(c)]; !ok {
				return nil, nil, nil, fmt.Errorf("foreign key %s column %s not in table %s", fk.Name, c, fk.Table)
			}
		}
	}

	var tables []schema.Table
	for _, t := range out {
		tables = append(tables, t)
	}
	return tables, idxs, fks, nil
}

func configTableToSchema(td TableDef) (schema.Table, error) {
	cols := make([]schema.Column, 0, len(td.Columns))
	for i, c := range td.Columns {
		col, err := buildColumn(c, i+1)
		if err != nil {
			return schema.Table{}, err
		}
		cols = append(cols, col)
	}
	return schema.Table{Name: td.Name, Columns: cols}, nil
}

func mergeTable(base schema.Table, td TableDef) (schema.Table, error) {
	idxMap := map[string]int{}
	for i, c := range base.Columns {
		idxMap[strings.ToLower(c.Name)] = i
	}
	for _, cdef := range td.Columns {
		key := strings.ToLower(cdef.Name)
		if i, ok := idxMap[key]; ok {
			col := base.Columns[i]
			if err := overrideColumn(&col, cdef); err != nil {
				return base, err
			}
			base.Columns[i] = col
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

func buildColumn(c ColumnDef, ordinal int) (schema.Column, error) {
	dt, err := buildDataType(c)
	if err != nil {
		return schema.Column{}, err
	}
	nullable := true
	if c.Nullable != nil {
		nullable = *c.Nullable
	}
	auto := false
	if c.AutoIncrement != nil {
		auto = *c.AutoIncrement
	}
	comp := false
	if c.Computed != nil {
		comp = *c.Computed
	}
	return schema.Column{
		ColumnID:   ordinal,
		Name:       c.Name,
		MaxLength:  intVal(c.Length),
		Precision:  intVal(c.Precision),
		Scale:      intVal(c.Scale),
		IsNullable: nullable,
		IsComputed: comp,
		IsAutoInc:  auto,
		Type:       c.Kind,
		Default:    c.Default,
		DataType:   dt,
	}, nil
}

func overrideColumn(dst *schema.Column, cdef ColumnDef) error {
	if cdef.Kind != "" {
		dt, err := buildDataType(cdef)
		if err != nil {
			return err
		}
		dst.DataType = dt
		dst.Type = cdef.Kind
	}
	if cdef.Length != nil {
		dst.MaxLength = *cdef.Length
		dst.DataType.Length = *cdef.Length
	}
	if cdef.Precision != nil {
		dst.Precision = *cdef.Precision
		dst.DataType.Precision = *cdef.Precision
	}
	if cdef.Scale != nil {
		dst.Scale = *cdef.Scale
		dst.DataType.Scale = *cdef.Scale
	}
	if cdef.Timezone != nil {
		dst.DataType.Timezone = *cdef.Timezone
	}
	if cdef.Nullable != nil {
		dst.IsNullable = *cdef.Nullable
	}
	if cdef.AutoIncrement != nil {
		dst.IsAutoInc = *cdef.AutoIncrement
	}
	if cdef.Computed != nil {
		dst.IsComputed = *cdef.Computed
	}
	if cdef.Default != "" {
		dst.Default = cdef.Default
	}
	return nil
}

func buildDataType(c ColumnDef) (schema.DataType, error) {
	kind := strings.ToLower(c.Kind)
	var dt schema.DataType
	switch kind {
	case "bool":
		dt.Kind = schema.KindBool
	case "int16":
		dt.Kind = schema.KindInt16
	case "int32":
		if boolVal(c.AutoIncrement) {
			dt.Kind = schema.KindSerialInt32
		} else {
			dt.Kind = schema.KindInt32
		}
	case "int64":
		if boolVal(c.AutoIncrement) {
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
	case "":
		return schema.DataType{}, fmt.Errorf("missing column kind")
	default:
		return schema.DataType{}, fmt.Errorf("unknown column kind: %s", c.Kind)
	}
	if c.Length != nil {
		dt.Length = *c.Length
	}
	if c.Precision != nil {
		dt.Precision = *c.Precision
	}
	if c.Scale != nil {
		dt.Scale = *c.Scale
	}
	if c.Timezone != nil {
		dt.Timezone = *c.Timezone
	}
	return dt, nil
}

func buildIndex(def IndexDef, tableName string) (schema.Index, error) {
	it, err := parseIndexType(def.Type)
	if err != nil {
		return schema.Index{}, err
	}
	return schema.Index{
		Table:          tableName,
		Name:           def.Name,
		Columns:        def.Columns,
		IncludeColumns: def.IncludeColumns,
		Filter:         def.Filter,
		IndexType:      it,
	}, nil
}

func buildForeignKey(def ForeignKeyDef, tableName string) (schema.ForeignKey, error) {
	if def.Name == "" {
		return schema.ForeignKey{}, fmt.Errorf("foreign key missing name")
	}
	return schema.ForeignKey{
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
	case "", "non_unique":
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

// Options converts basic config-level fields into migrate.Options.
// (Does not yet pass custom tables/indexes directly.)
func (c *Root) Options() []migrate.Option {
	return []migrate.Option{
		migrate.WithSourceURL(c.Source.URL),
		migrate.WithTargetURL(c.Target.URL),
		migrate.WithIncludeData(c.Include.Data),
		migrate.WithIncludeTables(c.Include.Tables),
		migrate.WithIncludeFuncs(c.Include.Functions),
		migrate.WithIncludeTrigs(c.Include.Triggers),
		migrate.WithIncludeProcs(c.Include.Procedures),
		migrate.WithIncludeViews(c.Include.Views),
		migrate.WithTextType(c.Target.TextType),
		migrate.WithDataBatchSize(c.Target.DataBatchSize),
	}
}
