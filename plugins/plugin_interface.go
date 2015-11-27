package plugins

import (
	"github.com/bbangert/toml"
)

type Input interface {
	Init(pcf *PluginCommonConfig, config toml.Primitive) error
	Run(in InputRunner) error
}

type Output interface {
	Init(pcf *PluginCommonConfig, config toml.Primitive) error
	Run(out OutputRunner) error
}

type Decoder interface {
	Init(config toml.Primitive) error
	Decode(pack *PipelinePack) (*PipelinePack, error)
}

type Encoder interface {
	Init(config toml.Primitive) error
	Encode(pack *PipelinePack) (*PipelinePack, error)
}

type PluginCommonConfig struct {
	Type    string `toml:"type"`
	Tag     string `toml:"tag"`
	Decoder string `toml:"decoder"`
	Encoder string `toml:"encoder"`
}
