package file

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/ActiveState/tail"
	"github.com/bbangert/toml"
)

type TailInputConfig struct {
	Path         string
	PosFile      string `toml:"pos_file"`
	SyncInterval int    `toml:"sync_interval"`
	OffsetValue  int64
}

type TailInput struct {
	config             *TailInputConfig
	common             *plugins.PluginCommonConfig
	checkpointFile     *os.File
	checkpointFilename string
}

func (this *TailInput) writeCheckpoint(offset int64) (err error) {
	if this.checkpointFile == nil {
		if this.checkpointFile, err = os.OpenFile(this.checkpointFilename,
			os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			return
		}
	}
	this.checkpointFile.Seek(0, 0)
	err = binary.Write(this.checkpointFile, binary.LittleEndian, &offset)
	return
}

func (this *TailInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	this.common = pcf
	this.config = &TailInputConfig{
		Path:         "/var/log/messages",
		PosFile:      "/tmp/tail.pos",
		SyncInterval: 2,
	}
	if err := toml.PrimitiveDecode(conf, this.config); err != nil {
		return fmt.Errorf("Can't unmarshal tail config: %s", err)
	}
	this.checkpointFilename = this.config.PosFile
	if fileExists(this.checkpointFilename) {
		if this.config.OffsetValue, err = readCheckpoint(this.checkpointFilename); err != nil {
			return fmt.Errorf("readCheckpoint %s", err)
		}
	} else {
		if err = os.MkdirAll(filepath.Dir(this.checkpointFilename), 0766); err != nil {
			return
		}
		this.config.OffsetValue = 0
	}
	return nil
}

func (this *TailInput) Run(runner plugins.InputRunner) (err error) {
	defer func() {

		if err := recover(); err != nil {
			log.Fatalln("recover panic at err:", err)
		}
		if this.checkpointFile != nil {
			this.checkpointFile.Close()
		}
	}()
	var seek int
	if this.config.OffsetValue > 0 {
		seek = os.SEEK_SET
	} else {
		seek = os.SEEK_END
	}
	t, err := tail.TailFile(this.config.Path, tail.Config{
		Poll:      true,
		ReOpen:    true,
		Follow:    true,
		MustExist: false,
		Location:  &tail.SeekInfo{int64(this.config.OffsetValue), seek},
	})
	if err != nil {
		return err
	}
	tick := time.NewTicker(time.Second * time.Duration(this.config.SyncInterval))
	count := 0

	for {
		select {
		case <-tick.C:
			{
				if count > 0 {
					offset, err := t.Tell()
					if err != nil {
						log.Println("Tell return error: ", err)
						continue
					}
					if err = this.writeCheckpoint(offset); err != nil {
						return err
					}

					count = 0
				}
			}
		case line := <-t.Lines:
			{
				pack := <-runner.InChan()
				pack.MsgBytes = []byte(line.Text)
				pack.Msg.Tag = this.common.Tag
				pack.Msg.Timestamp = time.Now().Unix()
				count++
				runner.RouterChan() <- pack
			}
		}
	}
	err = t.Wait()
	return err

}

func readCheckpoint(filename string) (offset int64, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	err = binary.Read(file, binary.LittleEndian, &offset)
	return
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func init() {
	plugins.RegisterInput("TailInput", func() interface{} {
		return new(TailInput)
	})
}
