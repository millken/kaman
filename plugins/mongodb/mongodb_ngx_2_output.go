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
"host_name":"cloud.vm",
"time":"Fri Apr 17 18:46:22 2015",
"attacks":[{
	"server_identity":"2qqkm.com",
	"number_attack_ip":1,
	"cc_attacks":[{
	"192.168.3.104":4}]
	}]
}
*/

type NgxAttackStatus struct {
	Time     string
	Attacks  []Attacks `json:"attacks"`
	HostName string    `json:"host_name"`
}

type Attacks struct {
	HostName       string
	ServerIdentity string         `json:"server_identity"`
	NumberAttackIp int64          `json:"number_attack_ip"`
	TopAttacksIp   map[string]int `json:"top_attacks_ip"`
	TopAttacksUrl  map[string]int `json:"top_attacks_url"`
	DateTime       time.Time
}

type MongodbNgx2Output struct {
	config      *MongodbOutputConfig
	FailedCount int64
}

func (self *MongodbNgx2Output) Init(conf toml.Primitive) error {
	log.Println("MongodbNgx2Output Init.")
	self.config = &MongodbOutputConfig{
		Host:       "localhost",
		Port:       "27017",
		Database:   "test",
		Collection: "test",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal MongodbNgx2Output config: %s", err)
	}
	return nil
}

func (self *MongodbNgx2Output) Run(runner plugins.OutputRunner) error {
	var _ngx NgxAttackStatus
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
		_ngx = NgxAttackStatus{}

		err := json.Unmarshal(pack.MsgBytes, &_ngx)
		if err != nil {
			log.Println("json.Unmarshal failed, err = ", err)
			continue
		}
		for _, server := range _ngx.Attacks {
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
	plugins.RegisterOutput("MongodbNgx2Output", func() interface{} {
		return new(MongodbNgx2Output)
	})
}
