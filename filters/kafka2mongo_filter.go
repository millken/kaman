package filters

import (
	//"log"
	"fmt"
	"reflect"
	"strings"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/ugorji/go/codec"
)

type Kafka2MongoFilter struct {
	codec *codec.JsonHandle
}

func (self *Kafka2MongoFilter) Run(pack *plugins.PipelinePack) (rpack *plugins.PipelinePack, err error) {
	rpack = pack
	msg := strings.SplitN(string(pack.MsgBytes), "\n", 2)
	if len(msg) != 2 {
		return nil, fmt.Errorf("Kafka2MongoFilter pack.MsgBytes length err , %s", string(pack.MsgBytes))
	}
	_codec := codec.JsonHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	self.codec = &_codec
	dec := codec.NewDecoderBytes([]byte(msg[1]), self.codec)
	err = dec.Decode(&rpack.Msg.Data)
	if err != nil {
		return
	}
	ip := rpack.Msg.Data["remote_ip"].(string)
	result, err := IpDb.Lookup(ip)
	if err != nil {
		return
	}
	rpack.Msg.Data["country"] = result.Country
	rpack.Msg.Data["city"] = result.City
	rpack.Msg.Data["isp"] = result.Isp
	//log.Printf("Kafka2MongoFilter : %q", rpack.Msg.Data)
	return rpack, nil
}

func init() {
	plugins.RegisterFilter("Kafka2MongoFilter", func() interface{} {
		return new(Kafka2MongoFilter)
	})
}
