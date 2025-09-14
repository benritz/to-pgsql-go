package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	cueast "cuelang.org/go/encoding/yaml"
)

// cueValidate loads the embedded config.cue schema in the module root and validates
// the provided raw YAML bytes against it. It returns an error if validation fails.
// This expects that config.cue uses the package name `config` and defines `config: #Config`.
func cueValidate(raw []byte) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Locate repository root containing config.cue (walk upward from wd).
	root := wd
	for {
		if _, err := os.Stat(filepath.Join(root, "config.cue")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root { // reached filesystem root
			return errors.New("could not locate config.cue in current or parent directories")
		}
		root = parent
	}

	cfg := &load.Config{Dir: root}
	insts := load.Instances([]string{"./config.cue"}, cfg)
	if len(insts) == 0 {
		return errors.New("no CUE instances found for config.cue")
	}
	inst := insts[0]
	if inst.Err != nil {
		return fmt.Errorf("loading config.cue: %w", inst.Err)
	}

	ctx := cuecontext.New()
	configSchema := ctx.BuildInstance(inst)
	if configSchema.Err() != nil {
		return fmt.Errorf("building config schema: %w", configSchema.Err())
	}

	yamlVal, err := cueast.Extract(filepath.Join(wd, "<input>"), raw)
	if err != nil {
		return fmt.Errorf("decode yaml to cue: %w", err)
	}
	dataVal := ctx.BuildFile(yamlVal)
	if dataVal.Err() != nil {
		return fmt.Errorf("building yaml cue value: %w", dataVal.Err())
	}

	configField := configSchema.LookupPath(cue.ParsePath("config"))
	if !configField.Exists() {
		return errors.New("config value not found in schema")
	}

	unified := configField.Unify(dataVal)
	if err := unified.Validate(); err != nil {
		return fmt.Errorf("cue validation error: %w", err)
	}
	return nil
}

// validateReader runs cue validation then rewinds reader for subsequent YAML decode.
func validateReader(r io.ReadSeeker) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := cueValidate(raw); err != nil {
		return err
	}
	// rewind so YAML decoder can read again
	_, err = r.Seek(0, io.SeekStart)
	return err
}
