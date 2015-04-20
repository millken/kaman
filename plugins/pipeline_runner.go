package plugins

import (
	"fmt"
	"log"
	"regexp"
	"sync/atomic"

	"github.com/bbangert/toml"
)

type PluginConfig map[string]toml.Primitive

var PluginTypeRegex = regexp.MustCompile("(Input|Output)$")

func getPluginType(pluginType string) string {
	pluginCats := PluginTypeRegex.FindStringSubmatch(pluginType)
	if len(pluginCats) < 2 {
		return ""
	}
	return pluginCats[1]
}

type Message struct {
	Tag       string
	Timestamp int64
	Data      map[string]interface{}
}

type PipelinePack struct {
	MsgBytes    []byte
	Msg         Message
	RecycleChan chan *PipelinePack
	RefCount    int32
}

func NewPipelinePack(recycleChan chan *PipelinePack) (pack *PipelinePack) {
	msgBytes := make([]byte, 100)
	data := make(map[string]interface{})
	msg := Message{Data: data}
	return &PipelinePack{
		MsgBytes:    msgBytes,
		Msg:         msg,
		RecycleChan: recycleChan,
		RefCount:    1,
	}
}

func (this *PipelinePack) Zero() {
	this.MsgBytes = this.MsgBytes[:cap(this.MsgBytes)]
	this.Msg.Data = make(map[string]interface{})
	this.RefCount = 1
}

func (this *PipelinePack) Recycle() {
	cnt := atomic.AddInt32(&this.RefCount, -1)
	if cnt == 0 {
		this.Zero()
		this.RecycleChan <- this
	}
}

type Pipeline struct {
	InputRunners  []interface{}
	OutputRunners []interface{}
	router        Router
}

func NewPipeLine() *Pipeline {
	config := new(Pipeline)
	config.router.Init()

	return config
}

func (this *Pipeline) LoadConfig(plugConfig map[string]toml.Primitive) error {
	for k, v := range plugConfig {

		plugCommon := &PluginCommonConfig{}
		if err := toml.PrimitiveDecode(v, plugCommon); err != nil {
			return fmt.Errorf("Can't unmarshal config: %s", err)
		}
		pluginType := getPluginType(plugCommon.Type)
		if pluginType == "" {
			continue
		}
		if plugCommon.Tag == "" {
			log.Println("Tag empty")
		}
		switch pluginType {
		case "Input":
			this.InputRunners = append(this.InputRunners, v)
		case "Output":
			this.OutputRunners = append(this.OutputRunners, v)
		}
		log.Printf("%s => %s", k, plugCommon.Type)
	}

	return nil
}

func (this *Pipeline) Run() {
	log.Println("Starting service...")

	PoolSize := 1000
	rChan := make(chan *PipelinePack, PoolSize)
	this.router.AddInChan(rChan)
	if len(this.InputRunners) == 0 {
		log.Fatalln("InputRunner requires that at least one")
	}

	for _, input_config := range this.InputRunners {
		cf := input_config.(toml.Primitive)

		InputRecycleChan := make(chan *PipelinePack, PoolSize)
		for i := 0; i < PoolSize; i++ {
			iPack := NewPipelinePack(InputRecycleChan)
			InputRecycleChan <- iPack
		}
		iRunner := NewInputRunner(InputRecycleChan, rChan)

		go iRunner.Start(cf)
	}

	for _, output_config := range this.OutputRunners {
		cf := output_config.(toml.Primitive)

		plugCommon := &PluginCommonConfig{
			Type: "",
			Tag:  "",
		}
		toml.PrimitiveDecode(cf, plugCommon)
		inChan := make(chan *PipelinePack, PoolSize)
		oRunner := NewOutputRunner(inChan)
		this.router.AddOutChan(plugCommon.Tag, oRunner.InChan())

		go oRunner.Start(cf)
	}

	this.router.Loop()
}
