package mongodb

import (
	//"encoding/json"
	"fmt"
	"log"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DnsQueryStats struct {
	Server, Domain, Month, Day, Hour string //server,domain
	//Nums int64
}

type MongodbDnsQueryOutput struct {
	config      *MongodbOutputConfig
	FailedCount int64
	outputExit  chan error
}

func (self *MongodbDnsQueryOutput) Init(conf toml.Primitive) error {
	log.Println("MongodbDnsQueryOutput Init.")
	self.config = &MongodbOutputConfig{
		Host:       "localhost",
		Port:       "27017",
		Database:   "test",
		Collection: "test",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal MongodbDnsQueryOutput config: %s", err)
	}
	self.outputExit = make(chan error)
	return nil
}

func (self *MongodbDnsQueryOutput) Run(runner plugins.OutputRunner) error {
	//[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
	var (
		ok        = true
		outBatch  = make(map[DnsQueryStats]int)
		batchData = make(map[DnsQueryStats]int)
		ticker    = time.Tick(time.Duration(2000) * time.Millisecond)
	)
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
			session.Refresh()
			batchData = outBatch
			outBatch = make(map[DnsQueryStats]int)
			collection := self.config.Collection + time.Now().Format("2006")
			coll := session.DB(self.config.Database).C(collection)
			for q, n := range batchData {
				_, err := coll.Upsert(q, bson.M{"$inc": bson.M{"querys": n}})
				if err != nil {
					log.Println(err)
				}
			}
		case err = <-self.outputExit:
			ok = false

		}
	}

	return nil
}

func init() {
	plugins.RegisterOutput("MongodbDnsQueryOutput", func() interface{} {
		return new(MongodbDnsQueryOutput)
	})
}
