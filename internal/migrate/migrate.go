package migrate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"benritz/topgsql/internal/dialect/mssql"
	"benritz/topgsql/internal/dialect/pgsql"
)

type Migration struct {
	sourceURL     string
	targetURL     string
	includeData   bool
	includeTables bool
	includeFuncs  bool
	includeTrigs  bool
	includeProcs  bool
	includeViews  bool
	textType      string
	dataBatchSize int
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
			indexes, err := source.GetIndexes(ctx)
			if err != nil {
				return err
			}
			if err := target.CreateIndexes(ctx, indexes); err != nil {
				return err
			}

			foreignKeys, err := source.GetForeignKeys(ctx)
			if err != nil {
				return err
			}
			if err := target.CreateForeignKeys(ctx, foreignKeys); err != nil {
				return err
			}
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
