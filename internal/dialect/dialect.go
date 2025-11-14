package dialect

import (
	"benritz/topgsql/internal/schema"
	"context"
	"os"
	"regexp"
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

var envVarPattern = regexp.MustCompile(`\{\{\s*Env\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// replace {{Env:XYZ}} with the value of the environment variable XYZ
func ExpandEnvMarkers(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(m string) string {
		sub := envVarPattern.FindStringSubmatch(m)
		if len(sub) == 2 {
			if v, ok := os.LookupEnv(sub[1]); ok {
				return v
			}
		}
		return m
	})
}
