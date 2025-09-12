package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"benritz/topgsql/internal/dialect/mssql"
	"benritz/topgsql/internal/dialect/pgsql"
)

var (
	sourceUrl     string
	targetUrl     string
	incData       bool
	incTables     bool
	incFunctions  bool
	incTriggers   bool
	incProcedures bool
	incViews      bool
	textType      string
	dataBatchSize int
)

func values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func main() {
	flag.StringVar(&sourceUrl, "source", "", "Source database connection URL")
	flag.StringVar(&targetUrl, "target", "", "Target file or database connection URL")
	flag.StringVar(&textType, "textType", "citext", "How to convert the text column schema. Either text, citext or varchar (default).")
	flag.BoolVar(&incData, "incData", false, "Include table data")
	flag.BoolVar(&incTables, "incTables", false, "Include tables schema")
	flag.BoolVar(&incFunctions, "incFunctions", false, "Include functions")
	flag.BoolVar(&incProcedures, "incProcedures", false, "Include procedures")
	flag.BoolVar(&incTriggers, "incTriggers", false, "Include triggers")
	flag.BoolVar(&incViews, "incViews", false, "Include views")
	flag.IntVar(&dataBatchSize, "dataBatchSize", 100, "Batch size for data inserts")
	flag.Parse()

	if sourceUrl == "" {
		fmt.Fprintln(os.Stderr, "Missing the source database connection URL")
		os.Exit(1)
	}

	ctx := context.Background()

	source, err := mssql.NewMssqlSource(sourceUrl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect to source:", err)
		os.Exit(1)
	}
	defer source.Close()

	target, err := pgsql.NewPgsqlTarget(ctx, targetUrl, textType)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect to target:", err)
		os.Exit(1)
	}
	defer target.Close(ctx)

	reader, err := source.NewTableDataReader(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create table data reader:", err)
		os.Exit(1)
	}

	if incTables || incData {
		tablesMap, err := source.GetTables(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		tables := values(tablesMap)
		sort.Slice(tables, func(i, j int) bool {
			return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
		})

		if incTables {
			if err := target.CreateTables(ctx, tables); err != nil {
				fmt.Fprintln(os.Stderr, "Failed to create table data:", err)
				os.Exit(1)
			}
		}

		if incData {
			if err := target.CopyTables(ctx, tables, reader); err != nil {
				fmt.Fprintln(os.Stderr, "Failed to copy table data:", err)
				os.Exit(1)
			}
		}

		if incTables {
			indexes, err := source.GetIndexes(ctx)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			if err := target.CreateIndexes(ctx, indexes); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			foreignKeys, err := source.GetForeignKeys(ctx)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			if err := target.CreateForeignKeys(ctx, foreignKeys); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	// if incFunctions {
	// }
	//
	// if incProcedures {
	// }
	//
	// if incViews {
	// }
	//
	// if incTriggers {
	// }
}
