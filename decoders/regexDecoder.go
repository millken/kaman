package decoders

import (
	"fmt"
	"regexp"

	"github.com/bbangert/toml"
	"github.com/millken/kaman/plugins"
)

type RegexDecoderConfig struct {
	MatchRegex string `toml:"match_regex"`
}

type RegexDecoder struct {
	Match  *regexp.Regexp
	config *RegexDecoderConfig
}

//http://play.golang.org/p/fOWJXgcfKO
func (this *RegexDecoder) Init(conf toml.Primitive) (err error) {
	this.config = &RegexDecoderConfig{}
	if err = toml.PrimitiveDecode(conf, this.config); err != nil {
		return fmt.Errorf("Can't unmarshal regexdecoder config: %s", err)
	}
	if this.Match, err = regexp.Compile(this.config.MatchRegex); err != nil {
		err = fmt.Errorf("RegexDecoder: %s", err)
		return
	}
	if this.Match.NumSubexp() == 0 {
		err = fmt.Errorf("RegexDecoder regex must contain capture groups")
		return
	}
	return nil
}

func (this *RegexDecoder) Decode(pack *plugins.PipelinePack) (rpack *plugins.PipelinePack, err error) {
	rpack = pack
	//if ok := this.Match.Match(rpack.MsgBytes); !ok {
	//	return nil, fmt.Error("%s not match `%s`", rpack.MsgBytes, this.config.MatchRegex)
	//}
	findResults := this.Match.FindStringSubmatch(string(pack.MsgBytes))
	if findResults == nil {
		return rpack, fmt.Errorf("%s not match `%s`", rpack.MsgBytes, this.config.MatchRegex)
	}
	for index, name := range this.Match.SubexpNames() {
		if index == 0 {
			continue
		}
		if name == "" {
			name = fmt.Sprintf("%d", index)
		}
		rpack.Msg.Data[name] = findResults[index]
	}
	//log.Printf("RegexDecoder : %#v", rpack.Msg.Data)
	return rpack, nil

}

func init() {
	plugins.RegisterDecoder("RegexDecoder", func() interface{} {
		return new(RegexDecoder)
	})
}
