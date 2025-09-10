package types

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
}

type Table struct {
	Name    string
	Columns []Column
}
