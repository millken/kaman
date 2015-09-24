package filters

//only for dnsquery input

import (
	//"log"
	"fmt"
	"reflect"
	"strings"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/ugorji/go/codec"
)

type Kafka3MongoFilter struct {
	codec *codec.JsonHandle
}

func (self *Kafka3MongoFilter) Run(pack *plugins.PipelinePack) (rpack *plugins.PipelinePack, err error) {
	rpack = pack
	msg := strings.SplitN(string(pack.MsgBytes), "\n", 2)
	if len(msg) != 2 {
		return nil, fmt.Errorf("Kafka3MongoFilter pack.MsgBytes length err , %s", string(pack.MsgBytes))
	}
	_codec := codec.JsonHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	self.codec = &_codec
	dec := codec.NewDecoderBytes([]byte(msg[1]), self.codec)
	err = dec.Decode(&rpack.Msg.Data)
	if err != nil {
		return
	}
	rpack.Msg.Data["primarydomain"] = GetPrimaryDomain(strings.TrimRight(rpack.Msg.Data["domain"].(string), "."))
	ips := strings.SplitN(rpack.Msg.Data["remote_ip"].(string), "|", 2)
	t, err := time.Parse("2006-01-02 15:04:05", rpack.Msg.Data["time"].(string))
	if err != nil {
		return nil, err
	}
	t2 := t.Add(time.Duration(8) * time.Hour)
	times := t2.Format("2006-01-02-15")
	rpack.Msg.Data["serverip"] = ips[1]
	//times := strings.Replace(rpack.Msg.Data["time"].(string)[0:13], " ", "-", 1)
	times_arr := strings.Split(times, "-")
	rpack.Msg.Data["year"] = times_arr[0]
	rpack.Msg.Data["month"] = times_arr[1]
	rpack.Msg.Data["day"] = times_arr[2]
	rpack.Msg.Data["hour"] = times_arr[3]
	rpack.Msg.Data["times"] = t2.Format("2006-01-02 15:00:00")
	//log.Printf("Kafka3MongoFilter : %q", rpack.Msg.Data)
	return rpack, nil
}

func init() {
	plugins.RegisterFilter("Kafka3MongoFilter", func() interface{} {
		return new(Kafka3MongoFilter)
	})
}
