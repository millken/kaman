package plugins

import (
	"log"
)

var decoder_plugins = make(map[string]func() interface{})
var decoders = make(map[string]Decoder)

func RegisterDecoder(name string, decoder func() interface{}) {
	if decoder == nil {
		log.Fatalln("decoder: Register decoder is nil")
	}

	if _, ok := decoder_plugins[name]; ok {
		log.Fatalln("decoder: Register called twice for decoder " + name)
	}
	log.Println("RegisterDecoder: ", name)

	decoder_plugins[name] = decoder
}

func PipeDecoder(name string, pack *PipelinePack) (rpack *PipelinePack, err error) {
	if decoder, ok := decoders[name]; ok {

		return decoder.Decode(pack)
	}
	pack.Msg.MsgBytes = pack.MsgBytes
	return pack, nil
}
