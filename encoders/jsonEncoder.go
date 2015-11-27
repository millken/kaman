package decoders

import (
	"encoding/json"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
	"github.com/ugorji/go/codec"
)

type JsonEncoderConfig struct {
}

type JsonEncoder struct {
	codec *codec.JsonHandle
}

func (this *JsonEncoder) Init(conf toml.Primitive) (err error) {
	//if err = toml.PrimitiveDecode(conf, this.config); err != nil {
	//	return fmt.Errorf("Can't unmarshal regexdecoder config: %s", err)
	//}
	return nil
}

func (this *JsonEncoder) Encode(pack *plugins.PipelinePack) (rpack *plugins.PipelinePack, err error) {
	rpack = pack
	js, err := json.Marshal(rpack.Msg.Data)
	if err != nil {
		return nil, err
	}
	rpack.MsgBytes = js
	return rpack, nil

}

func init() {
	plugins.RegisterEncoder("JsonEncoder", func() interface{} {
		return new(JsonEncoder)
	})
}
