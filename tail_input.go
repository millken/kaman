package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"strings"

	"github.com/bbangert/toml"
)

type TailInputConfig struct {
	Path string
	PosFile  string    `toml:"pos_file"`
}

type TailInput struct {
	config             *TailInputConfig
	common *PluginCommonConfig
}

func (this *TailInput) Init( pcf *PluginCommonConfig, conf toml.Primitive) (err error) {
		this.common = pcf
		this.config = &TailInputConfig{
		Path:                   "/var/log/messages",
		PosFile:                "/tmp/tail.pos",
		}	
		if err := toml.PrimitiveDecode(conf, this.config); err != nil {
			return fmt.Errorf("Can't unmarshal tail config: %s", err)
		}
		return nil
}

func (this *TailInput) Run(runner InputRunner) (err error) {
	var osize, nsize int64
	buf := make([]byte, 1)
	defer func() {
		if err := recover(); err != nil {
			logs.Fatalln("recover panic at err:", err)
		}
	}()
	fname := this.config.Path
	if file, err := os.Open(fname); err == nil {
		defer file.Close()

		if fi, err := file.Stat(); err == nil {
			osize = fi.Size()
			for {
				row := make([]string, 0)
				finfo, err := os.Stat(fname)
				if err != nil {
					log.Printf("finfo err = %q", err)
				}
				nsize = finfo.Size()
				if osize > nsize {
					break;
				}
				//log.Printf("osize=%d, nsize=%d  finfo.size=%d", osize, nsize,  finfo.Size())
				if nsize != osize {
					file.Seek(osize, 0)
					for {
						n, _ := file.Read(buf)
						if n == 0 || string(buf) == "\n" {
							break
						}
						row = append(row, string(buf))
						
					}
					pack := <-runner.InChan()
					pack.MsgBytes = []byte(strings.Join(row, ""))
					pack.Msg.Tag = this.common.Tag
					pack.Msg.Timestamp = time.Now().Unix()
					runner.RouterChan() <- pack
					osize = nsize
				}
				
				time.Sleep(250 * time.Millisecond)

			}
		}
	}
	return
}

func init() {
	RegisterInput("TailInput", func() interface{} {
		return new(TailInput)
	})
}
