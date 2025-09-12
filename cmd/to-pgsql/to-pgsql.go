package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"benritz/topgsql/internal/migrate"
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

	ctx := context.Background()

	migration, err := migrate.New(
		migrate.WithSourceURL(sourceUrl),
		migrate.WithTargetURL(targetUrl),
		migrate.WithIncludeData(incData),
		migrate.WithIncludeTables(incTables),
		migrate.WithIncludeFuncs(incFunctions),
		migrate.WithIncludeTrigs(incTriggers),
		migrate.WithIncludeProcs(incProcedures),
		migrate.WithIncludeViews(incViews),
		migrate.WithTextType(textType),
		migrate.WithDataBatchSize(dataBatchSize),
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	if err := migration.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "migration error: %v\n", err)
		os.Exit(1)
	}
}
