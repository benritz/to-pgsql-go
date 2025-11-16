package schema

import (
	"slices"
	"strings"
)

type DataTypeKind string

const (
	KindUnknown     DataTypeKind = "unknown"
	KindBool        DataTypeKind = "bool"
	KindInt16       DataTypeKind = "int16"
	KindInt32       DataTypeKind = "int32"
	KindInt64       DataTypeKind = "int64"
	KindSerialInt32 DataTypeKind = "serial_int32"
	KindSerialInt64 DataTypeKind = "serial_int64"
	KindFloat32     DataTypeKind = "float32"
	KindFloat64     DataTypeKind = "float64"
	KindNumeric     DataTypeKind = "numeric"
	KindMoney       DataTypeKind = "money"
	KindUUID        DataTypeKind = "uuid"
	KindVarChar     DataTypeKind = "varchar"
	KindText        DataTypeKind = "text"
	KindBinary      DataTypeKind = "binary"
	KindDate        DataTypeKind = "date"
	KindTime        DataTypeKind = "time"
	KindTimestamp   DataTypeKind = "timestamp"
	KindJson        DataTypeKind = "json"
)

type DataType struct {
	Kind      DataTypeKind
	Length    int
	Precision int
	Scale     int
	Timezone  bool
	Raw       string
}

type Column struct {
	ColumnID   int
	Name       string
	DataType   DataType
	MaxLength  int
	Precision  int
	Scale      int
	IsNullable bool
	IsComputed bool
	IsAutoInc  bool
	Default    string
}

type Table struct {
	Name         string
	Columns      []*Column
	Indexes      []*Index
	ForeignKeyes []*ForeignKey
	PKColNames   []string
}

func (t *Table) GetColumn(name string) *Column {
	for i, col := range t.Columns {
		if col.Name == name {
			return t.Columns[i]
		}
	}
	return nil
}

type IndexType string

const (
	IndexTypePrimaryKey       IndexType = "primary_key"
	IndexTypeUniqueConstraint IndexType = "unique_constraint"
	IndexTypeUnique           IndexType = "unique"
	IndexTypeNonUnique        IndexType = "non_unique"
)

type Index struct {
	Table          string
	Name           string
	Columns        []string
	IncludeColumns []string
	Filter         string
	IndexType      IndexType
}

type ForeignKey struct {
	Name              string
	Table             string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

type Function struct {
	Name       string
	Definition string
}

type Procedure struct {
	Name       string
	Definition string
}

type View struct {
	Name       string
	Definition string
}

type Trigger struct {
	Name       string
	Definition string
}

func PrimaryKeyColumns(table *Table) []string {
	for _, idx := range table.Indexes {
		if idx.IndexType == IndexTypePrimaryKey {
			return idx.Columns
		}
	}
	return nil
}

func UpdateableColumns(table *Table) []*Column {
	cols := []*Column{}
	for _, col := range table.Columns {
		if !col.IsComputed {
			cols = append(cols, col)
		}
	}
	return cols
}

func MissingForeignKeys(source, target *Table) []*ForeignKey {
	var missing []*ForeignKey
	for _, src := range source.ForeignKeyes {
		exists := slices.ContainsFunc(
			target.ForeignKeyes,
			func(tgt *ForeignKey) bool {
				return strings.EqualFold(src.Name, tgt.Name)
			},
		)
		if !exists {
			missing = append(missing, src)
		}
	}
	return missing
}

func MissingIndexes(source, target *Table) []*Index {
	var missing []*Index
	for _, src := range source.Indexes {
		exists := slices.ContainsFunc(
			target.Indexes,
			func(tgt *Index) bool {
				return strings.EqualFold(src.Name, tgt.Name)
			},
		)
		if !exists {
			missing = append(missing, src)
		}
	}
	return missing
}
