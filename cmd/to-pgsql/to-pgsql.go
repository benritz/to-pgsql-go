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
	incData       string
	incTables     string
	incFunctions  bool
	incTriggers   bool
	incProcedures bool
	incViews      bool
	incScripts    bool
	textType      string
	dataBatchSize int
)

func configOptions(c *config.Root) ([]migrate.Option, error) {
	var opts []migrate.Option

	opts = append(opts, migrate.WithSourceURL(c.Source.URL))
	opts = append(opts, migrate.WithTargetURL(c.Target.URL))
	opts = append(opts, migrate.WithTableDefs(c.Schema.Tables))

	opt, err := migrate.WithIncludeTables(c.Include.Tables)
	if err != nil {
		return opts, err
	}
	opts = append(opts, opt)

	opt, err = migrate.WithIncludeData(c.Include.Data)
	if err != nil {
		return opts, err
	}
	opts = append(opts, opt)

	opts = append(opts, migrate.WithIncludeFuncs(c.Include.Functions))
	opts = append(opts, migrate.WithIncludeTrigs(c.Include.Triggers))
	opts = append(opts, migrate.WithIncludeProcs(c.Include.Procedures))
	opts = append(opts, migrate.WithIncludeViews(c.Include.Views))
	opts = append(opts, migrate.WithIncludeScripts(c.Include.Scripts))
	opts = append(opts, migrate.WithScripts(c.Scripts, c.ScriptsPath))
	opts = append(opts, migrate.WithTextType(c.Target.TextType))
	opts = append(opts, migrate.WithDataBatchSize(c.Target.DataBatchSize))

	return opts, nil
}

func main() {
	flag.StringVar(&configPath, "config", "", "Config file")
	flag.StringVar(&sourceUrl, "source", "", "Source database connection URL")
	flag.StringVar(&targetUrl, "target", "", "Target file or database connection URL")
	flag.StringVar(&textType, "textType", "citext", "How to convert the text column schema. Either text, citext or varchar (default).")
	flag.StringVar(&incTables, "incTables", "none", "Include tables schema. Either none (default), create or recreate")
	flag.StringVar(&incData, "incData", "none", "Include table data. Either none (default), insert, overwrite or merge.")
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

		configOpts, err := configOptions(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			os.Exit(1)
		}
		opts = append(opts, configOpts...)
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
		opt, err := migrate.WithIncludeTables(config.TableAction(incTables))
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			os.Exit(1)
		}
		opts = append(opts, opt)
	}
	if _, ok := flagsSet["incData"]; ok {
		opt, err := migrate.WithIncludeData(config.DataAction(incData))
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
			os.Exit(1)
		}
		opts = append(opts, opt)
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
