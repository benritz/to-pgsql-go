package config

import (
	_ "embed"
	"errors"
	"fmt"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/encoding/yaml"
)

//go:embed config.cue
var configCue string

func validateBytes(data []byte) error {
	cueCtx := cuecontext.New()
	schema := cueCtx.CompileString(configCue)
	if schema.Err() != nil {
		return fmt.Errorf("building config schema: %w", schema.Err())
	}

	yamlFile, err := yaml.Extract("<input>", data)
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
