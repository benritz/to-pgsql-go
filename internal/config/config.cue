package config

import "strings"

#Config: {
	source: {
		url?: string
	}

	target: {
		url?:             string
		text_type?:       *"text" | "text" | "citext" | "varchar" | ""
		data_batch_size?: int & >=0
	}

	include?: {
		data?:       *"none" | "none" | "overwrite" | "merge"
		tables?:     bool
		functions?:  bool
		triggers?:   bool
		procedures?: bool
		views?:      bool
		scripts?:    bool
	}

	schema?: {
		tables?: [...#Table]
	}

	scripts?: {[...string & !=""]}
	scripts_base_path?: string & !=""
}

#Table: {
	name:      string & !=""
	strategy?: *"replace" | "merge" | "replace" | ""

	columns?: [...#Column]
	indexes?: [...#Index]
	foreign_keys?: [...#ForeignKey]

	// ---- Intra-table validations ----
	if columns != _|_ {
		_#colMap: {for c in columns {"\(strings.ToLower(c.name))": true}}
		_#uniqueCols?: len(_#colMap) == len(columns)
	}

	// Index columns must exist.
	if columns != _|_ && indexes != _|_ {
		_#colMap: {for c in columns {"\(strings.ToLower(c.name))": true}}
		_#indexColsExist: [for ix in indexes for c in ix.columns {_#colMap[strings.ToLower(c)]}]
		_#indexIncludeColsExist?: [for ix in indexes if ix.include_columns != _|_ for c in ix.include_columns {_#colMap[strings.ToLower(c)]}]
	}

	// Foreign key local columns must exist.
	if columns != _|_ && foreign_keys != _|_ {
		_#colMap: {for c in columns {"\(strings.ToLower(c.name))": true}}
		_#fkLocalColsExist: [for fk in foreign_keys for c in fk.columns {_#colMap[strings.ToLower(c)]}]
	}
}

#Column: {
	name: string & !=""

	kind: "bool" | "int16" | "int32" | "int64" | "serial_int32" | "serial_int64" | "float32" | "float64" | "numeric" | "money" | "uuid" | "varchar" | "text" | "binary" | "date" | "time" | "timestamp" | "json"

	length?:    int & >=0
	precision?: int & >=0
	scale?:     int & >=0 & <=precision
	if scale != _|_ {precision != _|_}

	timezone?:       bool
	nullable?:       bool
	auto_increment?: bool
	computed?:       bool
	default?:        string
}

#Index: {
	name:  string & !=""
	type?: *"non_unique" | "primary_key" | "unique_constraint" | "unique" | "non_unique"
	columns: [string & !="", ...string & !=""]
	include_columns?: [...string & !=""]
	filter?: string
}

#ForeignKey: {
	name: string & !=""
	columns: [string & !="", ...string & !=""]
	referenced_table: string & !=""
	referenced_columns: [string & !="", ...string & !=""]
}

config: #Config
