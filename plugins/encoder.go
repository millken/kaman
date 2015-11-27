package plugins

import (
	"log"
)

var encoder_plugins = make(map[string]func() interface{})
var encoders = make(map[string]Encoder)

func RegisterEncoder(name string, Encoder func() interface{}) {
	if Encoder == nil {
		log.Fatalln("encoder: Register encoder is nil")
	}

	if _, ok := encoder_plugins[name]; ok {
		log.Fatalln("encoder: Register called twice for encoder " + name)
	}
	log.Println("RegisterEncoder: ", name)

	encoder_plugins[name] = Encoder
}

func PipeEncoder(name string, pack *PipelinePack) (rpack *PipelinePack, err error) {
	if encoder, ok := encoders[name]; ok {

		return encoder.Encode(pack)
	}
	return pack, nil
}
