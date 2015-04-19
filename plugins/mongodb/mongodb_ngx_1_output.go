package mongodb

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
	mgo "gopkg.in/mgo.v2"
)

/*
{
    "time": "Wed Apr 15 16:09:45 2015",
    "servers": [{
        "host_name": "cloud.vm",
        "server_identity": "_server",
        "req": 1,
        "total_req": 2,
        "normal_req": 1,
        "total_normal_req": 2,
        "attacks": 0,
        "total_attacks": 0,
        "trigger_verify": 0,
        "total_trigger_verify": 0,
        "passed_verify": 0,
        "total_passed_verify": 0,
        "bytes_recv": 200,
        "total_bytes_recv": 200,
        "bytes_send": 181,
        "total_bytes_send": 181,
        "bytes_cached": 0,
        "total_bytes_cached": 0
    },
    {
        "host_name": "cloud.vm",
        "server_identity": "_",
        "req": 1,
        "total_req": 2,
        "normal_req": 1,
        "total_normal_req": 2,
        "attacks": 0,
        "total_attacks": 0,
        "trigger_verify": 0,
        "total_trigger_verify": 0,
        "passed_verify": 0,
        "total_passed_verify": 0,
        "bytes_recv": 200,
        "total_bytes_recv": 200,
        "bytes_send": 181,
        "total_bytes_send": 181,
        "bytes_cached": 0,
        "total_bytes_cached": 0
    }]
}
*/

type NgxStatus struct {
	Time     string
	Servers  []NgxStatusServers
	HostName string `json:"host_name"`
}

type NgxStatusServers struct {
	HostName           string
	ServerIdentity     string `json:"server_identity"`
	Req                int64
	TotalReq           int64 `json:"total_req"`
	NormalReq          int64 `json:"normal_req"`
	TotalNormalReq     int64 `json:"total_normal_req"`
	Attacks            int64 `json:"attacks"`
	TotalAttacks       int64 `json:"total_attacks"`
	TriggerVerify      int64 `json:"trigger_verify"`
	TotalTriggerVerify int64 `json:"total_trigger_verify"`
	PassedVerify       int64 `json:"passed_verify"`
	TotalPassedVerify  int64 `json:"total_passed_verify"`
	BytesRecv          int64 `json:"bytes_recv"`
	TotalBytesRecv     int64 `json:"total_bytes_recv"`
	BytesSend          int64 `json:"bytes_send"`
	TotalBytesSend     int64 `json:"total_bytes_send"`
	BytesCached        int64 `json:"bytes_cached"`
	TotalBytesCached   int64 `json:"total_bytes_cached"`
	DateTime           time.Time
}

type MongodbNgx1Output struct {
	config      *MongodbOutputConfig
	FailedCount int64
}

func (self *MongodbNgx1Output) Init(conf toml.Primitive) error {
	log.Println("MongodbNgx1Output Init.")
	self.config = &MongodbOutputConfig{
		Host:       "localhost",
		Port:       "27017",
		Database:   "test",
		Collection: "test",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal MongodbNgx1Output config: %s", err)
	}
	return nil
}

func (self *MongodbNgx1Output) Run(runner plugins.OutputRunner) error {
	var _ngx NgxStatus
	//[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]

	url := "mongodb://"
	if len(self.config.User) != 0 && len(self.config.Password) != 0 {
		url += self.config.User + ":" + self.config.Password + "@"
	}
	url += self.config.Host + ":" + self.config.Port + "/" + self.config.Database
	session, err := mgo.Dial(url)
	if err != nil {
		log.Println("mgo.Dial failed, err:", err)
		return err
	}

	info := &mgo.CollectionInfo{
		Capped:   self.config.Capped,
		MaxBytes: self.config.CappedSize * 1024 * 1024,
	}

	coll := session.DB(self.config.Database).C(self.config.Collection)
	err = coll.Create(info)
	if err != nil && err.Error() != "collection already exists" {
		return err
	}

	for {
		session.Refresh()
		coll := session.DB(self.config.Database).C(self.config.Collection)
		pack := <-runner.InChan()

		err := json.Unmarshal(pack.MsgBytes, &_ngx)
		if err != nil {
			log.Println("json.Unmarshal failed, err = ", err)
			continue
		}
		for _, server := range _ngx.Servers {
			if server.ServerIdentity == "_" {
				continue
			}
			server.HostName = _ngx.HostName
			server.DateTime = time.Now()
			err = coll.Insert(server)
			if err != nil {
				self.FailedCount++
				log.Println("insert failed, count=", self.FailedCount, "err:", err)
				continue
			}
		}

		pack.Recycle()
	}

	return nil
}

func init() {
	plugins.RegisterOutput("MongodbNgx1Output", func() interface{} {
		return new(MongodbNgx1Output)
	})
}
