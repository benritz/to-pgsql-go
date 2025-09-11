package schema

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
	MaxLength  int
	Precision  int
	Scale      int
	IsNullable bool
	IsComputed bool
	IsAutoInc  bool
	Type       string
	Default    string
	DataType   DataType
}

type Table struct {
	Name    string
	Columns []Column
}

type IndexType string

const (
	IndexTypePrimaryKey       IndexType = "primary_key"
	IndexTypeUniqueConstraint IndexType = "unique_constraint"
	IndexTypeUnique           IndexType = "unique"
	IndexTypeNonUnique        IndexType = "non_unique"
)

type Index struct {
	Table     string
	Name      string
	Columns   []string
	IndexType IndexType
}

type ForeignKey struct {
	Name              string
	Table             string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}
