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
	"cuelang.org/go/encoding/yaml"
)

func validateBytes(raw []byte) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	root := wd
	for {
		if _, err := os.Stat(filepath.Join(root, "config.cue")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			return errors.New("could not locate config.cue in current or parent directories")
		}
		root = parent
	}

	cfg := &load.Config{Dir: root}
	insts := load.Instances([]string{"./config.cue"}, cfg)
	if len(insts) == 0 {
		return errors.New("no cue instances found for config.cue")
	}
	inst := insts[0]
	if inst.Err != nil {
		return fmt.Errorf("loading config.cue: %w", inst.Err)
	}

	cueCtx := cuecontext.New()
	schema := cueCtx.BuildInstance(inst)
	if schema.Err() != nil {
		return fmt.Errorf("building config schema: %w", schema.Err())
	}

	yamlFile, err := yaml.Extract(filepath.Join(wd, "<input>"), raw)
	if err != nil {
		return fmt.Errorf("decode yaml to cue: %w", err)
	}

	yamlData := cueCtx.BuildFile(yamlFile)
	if yamlData.Err() != nil {
		return fmt.Errorf("building yaml cue value: %w", yamlData.Err())
	}

	configField := schema.LookupPath(cue.ParsePath("config"))
	if !configField.Exists() {
		return errors.New("config value not found in schema")
	}

	unified := configField.Unify(yamlData)
	if err := unified.Validate(); err != nil {
		return fmt.Errorf("validation error: %w", err)
	}
	return nil
}

func validateReader(r io.ReadSeeker) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := validateBytes(raw); err != nil {
		return err
	}
	_, err = r.Seek(0, io.SeekStart)
	return err
}
