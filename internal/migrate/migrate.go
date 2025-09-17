package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"benritz/topgsql/internal/config"
	"benritz/topgsql/internal/dialect/mssql"
	"benritz/topgsql/internal/dialect/pgsql"
)

type IncDataType string

const (
	IncDataNone   IncDataType = "none"
	IncDataInsert IncDataType = "insert"
	IncDataMerge  IncDataType = "merge"
)

type Migration struct {
	sourceURL       string
	targetURL       string
	includeData     bool
	includeTables   bool
	includeFuncs    bool
	includeTrigs    bool
	includeProcs    bool
	includeViews    bool
	includeScripts  bool
	textType        string
	dataBatchSize   int
	tableDefs       []config.TableDef
	scripts         []string
	scriptsBasePath string
}

type Option func(*Migration)

func New(opts ...Option) (*Migration, error) {
	m := Migration{
		textType: "citext",
	}

	for _, opt := range opts {
		opt(&m)
	}

	if m.sourceURL == "" {
		return nil, fmt.Errorf("missing source database connection URL")
	}

	switch m.textType {
	case "varchar", "text", "citext":
		break
	default:
		return nil, fmt.Errorf("invalid text type: %s", m.textType)
	}

	return &m, nil
}

func WithSourceURL(u string) Option {
	return func(m *Migration) {
		m.sourceURL = u
	}
}

func WithTargetURL(u string) Option {
	return func(m *Migration) {
		m.targetURL = u
	}
}

func WithIncludeData(v bool) Option {
	return func(m *Migration) {
		m.includeData = v
	}
}

func WithIncludeTables(v bool) Option {
	return func(m *Migration) {
		m.includeTables = v
	}
}

func WithIncludeFuncs(v bool) Option {
	return func(m *Migration) {
		m.includeFuncs = v
	}
}

func WithIncludeTrigs(v bool) Option {
	return func(m *Migration) {
		m.includeTrigs = v
	}
}

func WithIncludeProcs(v bool) Option {
	return func(m *Migration) {
		m.includeProcs = v
	}
}

func WithIncludeViews(v bool) Option {
	return func(m *Migration) {
		m.includeViews = v
	}
}

func WithIncludeScripts(v bool) Option {
	return func(m *Migration) {
		m.includeScripts = v
	}
}

func WithTextType(t string) Option {
	return func(m *Migration) {
		m.textType = t
	}
}

func WithDataBatchSize(size int) Option {
	return func(m *Migration) {
		m.dataBatchSize = size
	}
}

func WithTableDefs(tableDefs []config.TableDef) Option {
	return func(m *Migration) {
		m.tableDefs = tableDefs
	}
}

func WithScripts(scripts []string, scriptsBasePath string) Option {
	return func(m *Migration) {
		m.scripts = scripts
		m.scriptsBasePath = scriptsBasePath
		m.includeScripts = len(scripts) > 0
	}
}

func (m Migration) Run(ctx context.Context) error {
	source, err := mssql.NewMssqlSource(m.sourceURL)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer source.Close()

	target, err := pgsql.NewPgsqlTarget(ctx, m.targetURL, m.textType)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer target.Close(ctx)

	reader, err := source.NewTableDataReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create table data reader: %w", err)
	}

	if m.includeTables || m.includeData {
		tablesMap, err := source.GetTables(ctx)
		if err != nil {
			return err
		}

		if len(m.tableDefs) > 0 {
			if tablesMap, err = config.BuildTables(m.tableDefs, tablesMap); err != nil {
				return err
			}
		}

		tables := values(tablesMap)
		sort.Slice(tables, func(i, j int) bool {
			return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
		})

		if m.includeTables {
			if err := target.CreateTables(ctx, tables); err != nil {
				return fmt.Errorf("failed to create table schema: %w", err)
			}
		}

		if m.includeData {
			if err := target.CopyTables(ctx, tables, reader); err != nil {
				return fmt.Errorf("failed to copy table data: %w", err)
			}
		}

		if m.includeTables {
			if err := target.CreateConstraintsAndIndexes(ctx, tables); err != nil {
				return fmt.Errorf("failed to create constraints and indexes: %w", err)
			}
		}
	}

	if m.includeViews {
		views, err := source.GetViews(ctx)
		if err != nil {
			return fmt.Errorf("failed to get views: %w", err)
		}

		if err := target.CreateViews(ctx, views); err != nil {
			return fmt.Errorf("failed to create views: %w", err)
		}
	}

	if m.includeTrigs {
		triggers, err := source.GetTriggers(ctx)
		if err != nil {
			return fmt.Errorf("failed to get triggers: %w", err)
		}

		if err := target.CreateTriggers(ctx, triggers); err != nil {
			return fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	if m.includeProcs {
		procedures, err := source.GetProcedures(ctx)
		if err != nil {
			return fmt.Errorf("failed to get procedures: %w", err)
		}

		if err := target.CreateProcedures(ctx, procedures); err != nil {
			return fmt.Errorf("failed to create procedures: %w", err)
		}
	}

	if m.includeFuncs {
		functions, err := source.GetFunctions(ctx)
		if err != nil {
			return fmt.Errorf("failed to get functions: %w", err)
		}

		if err := target.CreateFunctions(ctx, functions); err != nil {
			return fmt.Errorf("failed to create functions: %w", err)
		}
	}

	if m.includeScripts && len(m.scripts) > 0 {
		var paths []string

		for _, script := range m.scripts {
			path := script
			if !filepath.IsAbs(path) {
				path = filepath.Join(m.scriptsBasePath, path)
			}
			paths = append(paths, path)
		}
		if err := target.CreateScripts(ctx, paths); err != nil {
			return fmt.Errorf("failed to create scripts: %w", err)
		}
	}

	return nil
}

func values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
