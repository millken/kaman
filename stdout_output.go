package main

import (
	"github.com/bbangert/toml"
	"log"
)

type StdoutOutput struct {
}

func (self *StdoutOutput) Init(conf toml.Primitive) error {
	return nil
}

func (self *StdoutOutput) Run(runner OutputRunner) error {

	for {
		pack := <-runner.InChan()
		log.Printf("stdout : %s\n", string(pack.MsgBytes))
		pack.Recycle()
	}

	return nil
}

func init() {
	RegisterOutput("StdoutOutput", func() interface{} {
		return new(StdoutOutput)
	})
}
