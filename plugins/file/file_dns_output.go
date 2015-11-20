package file

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
)

type DnsQueryStats struct {
	Server, Domain, Month, Day, Hour string //server,domain
}

type FileDnsStatsOutputConfig struct {
	Path string
}

type FileDnsStatsOutput struct {
	config      *FileDnsStatsOutputConfig
	FailedCount int64
	outputExit  chan error
}

func (self *FileDnsStatsOutput) Init(conf toml.Primitive) error {
	log.Println("FileDnsStatsOutput Init.")
	self.config = &FileDnsStatsOutputConfig{
		Path: "/dev/shm/dnsquery/",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal FileDnsStatsOutput config: %s", err)
	}
	if err := os.MkdirAll(filepath.Dir(self.config.Path), 0766); err != nil {
		return err
	}
	self.outputExit = make(chan error)
	return nil
}

func (self *FileDnsStatsOutput) Run(runner plugins.OutputRunner) error {
	var (
		ok        = true
		outBatch  = make(map[DnsQueryStats]int)
		batchData = make(map[DnsQueryStats]int)
		ticker    = time.Tick(time.Duration(180) * time.Second)
	)

	for ok {
		select {
		case pack := <-runner.InChan():
			dnsquery := DnsQueryStats{
				Server: pack.Msg.Data["serverip"].(string),
				Domain: pack.Msg.Data["primarydomain"].(string),
				Month:  pack.Msg.Data["month"].(string),
				Day:    pack.Msg.Data["day"].(string),
				Hour:   pack.Msg.Data["hour"].(string),
			}
			if num := outBatch[dnsquery]; num > 0 {
				outBatch[dnsquery] = num + 1
			} else {
				outBatch[dnsquery] = 1
			}
			pack.Recycle()
		case <-ticker:
			batchData = outBatch
			outBatch = make(map[DnsQueryStats]int)
			cotents := ""
			for q, n := range batchData {
				cotents = cotents + fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%d\n",
					q.Server, q.Domain, q.Month, q.Day, q.Hour, n)
			}
			dfile, err := os.OpenFile(fmt.Sprintf("%s/%s", self.config.Path, time.Now().Format("01-02 15:04:05")),
				os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				return err
			}
			defer dfile.Close()
			dfile.WriteString(cotents)

		}
	}

	return nil
}

func init() {
	plugins.RegisterOutput("FileDnsStatsOutput", func() interface{} {
		return new(FileDnsStatsOutput)
	})
}
