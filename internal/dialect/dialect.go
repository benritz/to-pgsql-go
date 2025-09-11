package dialect

import (
	"benritz/topgsql/internal/schema"
	"context"
)

type TableDataReader interface {
	Open(ctx context.Context, table string, cols []schema.Column) error
	ReadRow() ([]any, error)
	Close(ctx context.Context) error
}
