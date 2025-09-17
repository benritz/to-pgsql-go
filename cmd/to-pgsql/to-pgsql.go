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
	incScripts    bool
	textType      string
	dataBatchSize int
)

func configOptions(c *config.Root) []migrate.Option {
	return []migrate.Option{
		migrate.WithSourceURL(c.Source.URL),
		migrate.WithTargetURL(c.Target.URL),
		migrate.WithTableDefs(c.Schema.Tables),
		migrate.WithIncludeData(c.Include.Data),
		migrate.WithIncludeTables(c.Include.Tables),
		migrate.WithIncludeFuncs(c.Include.Functions),
		migrate.WithIncludeTrigs(c.Include.Triggers),
		migrate.WithIncludeProcs(c.Include.Procedures),
		migrate.WithIncludeViews(c.Include.Views),
		migrate.WithIncludeScripts(c.Include.Scripts),
		migrate.WithScripts(c.Scripts, c.ScriptsBasePath),
		migrate.WithTextType(c.Target.TextType),
		migrate.WithDataBatchSize(c.Target.DataBatchSize),
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
	flag.BoolVar(&incScripts, "incScripts", false, "Include scripts")
	flag.IntVar(&dataBatchSize, "dataBatchSize", 0, "Batch size for data inserts")
	flag.Parse()

	flagsSet := make(map[string]struct{})
	flag.Visit(func(f *flag.Flag) {
		flagsSet[f.Name] = struct{}{}
	})

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
	if _, ok := flagsSet["incTables"]; ok {
		opts = append(opts, migrate.WithIncludeTables(incTables))
	}
	if _, ok := flagsSet["incData"]; ok {
		opts = append(opts, migrate.WithIncludeData(incData))
	}
	if _, ok := flagsSet["incFunctions"]; ok {
		opts = append(opts, migrate.WithIncludeFuncs(incFunctions))
	}
	if _, ok := flagsSet["incTriggers"]; ok {
		opts = append(opts, migrate.WithIncludeTrigs(incTriggers))
	}
	if _, ok := flagsSet["incProcedures"]; ok {
		opts = append(opts, migrate.WithIncludeProcs(incProcedures))
	}
	if _, ok := flagsSet["incViews"]; ok {
		opts = append(opts, migrate.WithIncludeViews(incViews))
	}
	if _, ok := flagsSet["incScripts"]; ok {
		opts = append(opts, migrate.WithIncludeScripts(incScripts))
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
