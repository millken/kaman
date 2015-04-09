package main

import (
	"log"
	"github.com/bbangert/toml"
)

type InputRunner interface {
	InChan() chan *PipelinePack
	RouterChan() chan *PipelinePack
	Start(cf toml.Primitive)
}

type iRunner struct {
	inChan     chan *PipelinePack
	routerChan chan *PipelinePack
}

func NewInputRunner(in, router chan *PipelinePack) InputRunner {
	return &iRunner{
		inChan:     in,
		routerChan: router,
	}
}

func (this *iRunner) InChan() chan *PipelinePack {
	return this.inChan
}

func (this *iRunner) RouterChan() chan *PipelinePack {
	return this.routerChan
}

func (this *iRunner) Start(conf toml.Primitive) {
		plugCommon := &PluginCommonConfig{
			Type : "",
			Tag : "",
		}
		if err := toml.PrimitiveDecode(conf, plugCommon); err != nil {
			log.Fatalln("toml struct error")
		}			

	input, ok := input_plugins[plugCommon.Type]
	if !ok {
		log.Fatalln("unkown type ", plugCommon.Type)
	}

	in := input()

	err := in.(Input).Init(plugCommon, conf)
	if err != nil {
		log.Fatalln("in.(Input).Init", err)
	}

	err = in.(Input).Run(this)
	if err != nil {
		log.Fatalln("in.(Input).Run", err)
	}
}

type OutputRunner interface {
	InChan() chan *PipelinePack
	Start(cf toml.Primitive)
}

type oRunner struct {
	inChan chan *PipelinePack
}

func NewOutputRunner(in chan *PipelinePack) OutputRunner {
	return &oRunner{
		inChan: in,
	}
}

func (this *oRunner) InChan() chan *PipelinePack {
	return this.inChan
}

func (this *oRunner) Start(cf toml.Primitive) {
		plugCommon := &PluginCommonConfig{
			Type : "",
			Tag : "",
		}
		if err := toml.PrimitiveDecode(cf, plugCommon); err != nil {
			log.Fatalln("toml struct error")
		}	

	output_plugin, ok := output_plugins[plugCommon.Type]
	if !ok {
		log.Fatalln("unkown type ", plugCommon.Type)
	}

	out := output_plugin()

	err := out.(Output).Init(cf)
	if err != nil {
		log.Fatalln("out.(Output).Init", err)
	}

	err = out.(Output).Run(this)
	if err != nil {
		log.Fatalln("out.(Output).Run", err)
	}
}
