package main
import (
	"github.com/bbangert/toml"
)

type Input interface {
	Init(pcf *PluginCommonConfig, config toml.Primitive) error
	Run(in InputRunner) error
}

type Output interface {
	Init(config toml.Primitive) error
	Run(out OutputRunner) error
}

type PluginCommonConfig struct {
	Type string `toml:"type"`
	Tag string `toml:"tag"`
}
