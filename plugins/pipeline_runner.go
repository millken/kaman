package plugins

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/bbangert/toml"
	notify "github.com/bitly/go-notify"
)

type PluginConfig map[string]toml.Primitive

var PluginTypeRegex = regexp.MustCompile("(Input|Output|Encoder|Decoder)$")

func getPluginType(pluginType string) string {
	pluginCats := PluginTypeRegex.FindStringSubmatch(pluginType)
	if len(pluginCats) < 2 {
		return ""
	}
	return pluginCats[1]
}

type Message struct {
	MsgBytes  []byte
	Tag       string
	Timestamp int64
	Data      map[string]interface{}
	sync.RWMutex
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
	msg := Message{
		MsgBytes: msgBytes,
		Data:     data,
	}
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
	this.Msg.MsgBytes = this.MsgBytes
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
	DecodeRunners []interface{}
	EncodeRunners []interface{}
	router        Router
}

func NewPipeLine() *Pipeline {
	config := new(Pipeline)
	config.router.Init()

	return config
}

func (this *Pipeline) LoadConfig(plugConfig map[string]toml.Primitive) error {
	for k, v := range plugConfig {
		log.Printf("v %+v", v)
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
		case "Encoder":
			this.EncodeRunners = append(this.EncodeRunners, v)
		case "Decoder":
			this.DecodeRunners = append(this.DecodeRunners, v)
		}
		log.Printf("%s => %s", k, plugCommon.Type)
	}

	return nil
}

func (this *Pipeline) Run(mc *MasterConfig) {
	log.Println("Starting service...")
	plugCommon := &PluginCommonConfig{
		Type:    "",
		Decoder: "",
		Tag:     "",
		Encoder: "",
	}
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

		toml.PrimitiveDecode(cf, plugCommon)
		inChan := make(chan *PipelinePack, PoolSize)
		oRunner := NewOutputRunner(inChan)
		this.router.AddOutChan(plugCommon.Tag, oRunner.InChan())

		go oRunner.Start(cf)
	}

	for _, encode_config := range this.EncodeRunners {
		toml.PrimitiveDecode(encode_config.(toml.Primitive), plugCommon)
		encoder_plugin, ok := encoder_plugins[plugCommon.Type]
		if !ok {
			log.Fatalln("unkown encoder ", plugCommon.Type)
		}
		encoder := encoder_plugin()

		err := encoder.(Encoder).Init(encode_config.(toml.Primitive))
		if err != nil {
			log.Fatalln("encoder.(Encoder).Init", err)
		}
		encoders[plugCommon.Encoder] = encoder.(Encoder)
	}

	for _, decode_config := range this.DecodeRunners {
		toml.PrimitiveDecode(decode_config.(toml.Primitive), plugCommon)
		decoder_plugin, ok := decoder_plugins[plugCommon.Type]
		if !ok {
			log.Fatalln("unkown decoder ", plugCommon.Type)
		}
		decoder := decoder_plugin()

		err := decoder.(Decoder).Init(decode_config.(toml.Primitive))
		if err != nil {
			log.Fatalln("decoder.(Decoder).Init", err)
		}
		decoders[plugCommon.Decoder] = decoder.(Decoder)
	}

	go this.router.Loop()
	this.SignalWorker()
}
func (this *Pipeline) SignalWorker() {
	// wait for sigint
	ok := true
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for ok {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGHUP:
				log.Println("Reload initiated.")
				if err := notify.Post("reload", nil); err != nil {
					log.Println("Error sending reload event: ", err)
				}
			case syscall.SIGINT, syscall.SIGTERM:
				log.Println("Shutdown initiated.")
				ok = false
			}
		}
	}
}
