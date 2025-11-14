package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"benritz/topgsql/internal/config"
	"benritz/topgsql/internal/dialect"
	"benritz/topgsql/internal/dialect/mssql"
	"benritz/topgsql/internal/dialect/pgsql"
)

type Migration struct {
	sourceURL        string
	targetURL        string
	includeData      config.DataAction
	includeTables    config.TableAction
	includeFuncs     bool
	includeTrigs     bool
	includeProcs     bool
	includeViews     bool
	textType         string
	dataBatchSize    int
	tableDefs        []config.TableDef
	scripts          []string
	scriptsBasePath  string
	scriptsExpandEnv bool
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

func WithSourceURL(v string) Option {
	return func(m *Migration) {
		m.sourceURL = v
	}
}

func WithTargetURL(v string) Option {
	return func(m *Migration) {
		m.targetURL = v
	}
}

func WithIncludeTables(v config.TableAction) (Option, error) {
	var err error

	if v != config.TableNone && v != config.TableCreate && v != config.TableRecreate {
		err = fmt.Errorf("invalid table action: %s", v)
		v = config.TableNone
	}

	opt := func(m *Migration) {
		m.includeTables = v
	}

	return opt, err
}

func WithIncludeData(v config.DataAction) (Option, error) {
	var err error

	if v != config.DataNone && v != config.DataInsert && v != config.DataOverwrite && v != config.DataMerge {
		err = fmt.Errorf("invalid data action: %s", v)
		v = config.DataNone
	}

	opt := func(m *Migration) {
		m.includeData = v
	}

	return opt, err
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

func WithScripts(
	scripts []string,
	basePath string,
	expandEnv bool,
) Option {
	return func(m *Migration) {
		m.scripts = scripts
		m.scriptsBasePath = basePath
		m.scriptsExpandEnv = expandEnv
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

	if m.includeTables != config.TableNone || m.includeData != config.DataNone {
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

		if m.includeTables != config.TableNone {
			recreate := m.includeTables == config.TableRecreate
			if err := target.CreateTables(ctx, tables, recreate); err != nil {
				return fmt.Errorf("failed to create table schema: %w", err)
			}
			if err := target.CreateConstraintsAndIndexes(ctx, tables, recreate); err != nil {
				return fmt.Errorf("failed to create constraints and indexes: %w", err)
			}
		}

		if m.includeData != config.DataNone {
			var copyAction dialect.CopyAction

			switch m.includeData {
			case config.DataInsert:
				copyAction = dialect.CopyInsert
			case config.DataOverwrite:
				copyAction = dialect.CopyOverwrite
			case config.DataMerge:
				copyAction = dialect.CopyMerge
			default:
				return fmt.Errorf("invalid data action")
			}

			if err := target.CopyTables(ctx, tables, reader, copyAction); err != nil {
				return fmt.Errorf("failed to copy table data: %w", err)
			}
		}

		// TODO - add option to defer index/key creation until after data load
		// if m.includeTables != config.TableNone {
		// 	recreate := m.includeTables == config.TableRecreate
		// 	if err := target.CreateConstraintsAndIndexes(ctx, tables, recreate); err != nil {
		// 		return fmt.Errorf("failed to create constraints and indexes: %w", err)
		// 	}
		// }
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

	if len(m.scripts) > 0 {
		var paths []string

		for _, script := range m.scripts {
			path := script
			if !filepath.IsAbs(path) {
				path = filepath.Join(m.scriptsBasePath, path)
			}
			paths = append(paths, path)
		}

		if err := target.CreateScripts(ctx, paths, m.scriptsExpandEnv); err != nil {
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
