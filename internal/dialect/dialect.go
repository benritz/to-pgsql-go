package dialect

import (
	"benritz/topgsql/internal/schema"
	"context"
	"strings"
)

type CopyAction int

const CopyInsert CopyAction = 1
const CopyOverwrite CopyAction = 2
const CopyMerge CopyAction = 3

type TableDataReader interface {
	Open(ctx context.Context, table string, cols []*schema.Column) error
	ReadRow() ([]any, error)
	Close(ctx context.Context) error
}

func StripNull(s string) string {
	return strings.Map(func(r rune) rune {
		if r == 0 {
			return -1 // drop
		}
		return r
	}, s)
}
