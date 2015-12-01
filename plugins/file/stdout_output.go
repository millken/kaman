package file

import (
	"log"

	"github.com/bbangert/toml"
	"github.com/millken/kaman/plugins"
)

type StdoutOutput struct {
	common *plugins.PluginCommonConfig
}

func (self *StdoutOutput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) error {
	self.common = pcf
	log.Printf("use decoder: %s", self.common.Decoder)
	return nil
}

func (self *StdoutOutput) Run(runner plugins.OutputRunner) (err error) {

	for {
		pack := <-runner.InChan()
		pack, err = plugins.PipeDecoder(self.common.Decoder, pack)
		if err != nil {
			log.Printf("PipeDecoder :%s", err)
			continue
		}
		pack, err = plugins.PipeEncoder(self.common.Encoder, pack)
		if err != nil {
			log.Printf("PipeEncoder :%s", err)
			continue
		}
		log.Printf("stdout : %s\n", pack.MsgBytes)
		pack.Recycle()
	}

	return nil
}

func init() {
	plugins.RegisterOutput("StdoutOutput", func() interface{} {
		return new(StdoutOutput)
	})
}
