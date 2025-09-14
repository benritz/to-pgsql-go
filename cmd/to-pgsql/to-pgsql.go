package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"benritz/topgsql/internal/config"
	"benritz/topgsql/internal/migrate"
)

var (
	configPath    string
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

func configOptions(c *config.Root) []migrate.Option {
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
		migrate.WithTableDefs(c.Schema.Tables),
	}
}

func main() {
	flag.StringVar(&configPath, "config", "", "Config file")
	flag.StringVar(&sourceUrl, "source", "", "Source database connection URL")
	flag.StringVar(&targetUrl, "target", "", "Target file or database connection URL")
	flag.StringVar(&textType, "textType", "citext", "How to convert the text column schema. Either text, citext or varchar (default).")
	flag.BoolVar(&incData, "incData", false, "Include table data")
	flag.BoolVar(&incTables, "incTables", false, "Include tables schema")
	flag.BoolVar(&incFunctions, "incFunctions", false, "Include functions")
	flag.BoolVar(&incProcedures, "incProcedures", false, "Include procedures")
	flag.BoolVar(&incTriggers, "incTriggers", false, "Include triggers")
	flag.BoolVar(&incViews, "incViews", false, "Include views")
	flag.IntVar(&dataBatchSize, "dataBatchSize", 0, "Batch size for data inserts")
	flag.Parse()

	opts := []migrate.Option{}

	if configPath != "" {
		cfg, err := config.LoadFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
			os.Exit(1)
		}

		opts = append(opts, configOptions(cfg)...)
	}

	if sourceUrl != "" {
		opts = append(opts, migrate.WithSourceURL(sourceUrl))
	}
	if targetUrl != "" {
		opts = append(opts, migrate.WithTargetURL(targetUrl))
	}
	if textType != "" {
		opts = append(opts, migrate.WithTextType(textType))
	}
	// For booleans we only override when true (can't detect 'explicit false' without pflags)
	if incData {
		opts = append(opts, migrate.WithIncludeData(true))
	}
	if incTables {
		opts = append(opts, migrate.WithIncludeTables(true))
	}
	if incFunctions {
		opts = append(opts, migrate.WithIncludeFuncs(true))
	}
	if incTriggers {
		opts = append(opts, migrate.WithIncludeTrigs(true))
	}
	if incProcedures {
		opts = append(opts, migrate.WithIncludeProcs(true))
	}
	if incViews {
		opts = append(opts, migrate.WithIncludeViews(true))
	}
	if dataBatchSize != 0 {
		opts = append(opts, migrate.WithDataBatchSize(dataBatchSize))
	}

	ctx := context.Background()

	migration, err := migrate.New(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	if err := migration.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "migration error: %v\n", err)
		os.Exit(1)
	}
}
