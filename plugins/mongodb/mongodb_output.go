package mongodb

import (
	"fmt"
	"log"

	"github.com/bbangert/toml"
	"github.com/millken/kaman/plugins"
	mgo "gopkg.in/mgo.v2"
)

type MongodbOutputConfig struct {
	Host, Port, Database, Collection string
	User, Password                   string
	Capped                           bool
	CappedSize                       int `toml:"capped_size"`
}

type MongodbOutput struct {
	config      *MongodbOutputConfig
	FailedCount int64
}

func (self *MongodbOutput) Init(conf toml.Primitive) error {
	log.Println("MongodbOutput Init.")
	self.config = &MongodbOutputConfig{
		Host:       "localhost",
		Port:       "27017",
		Database:   "test",
		Collection: "test",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal MongodbOutput config: %s", err)
	}
	return nil
}

func (self *MongodbOutput) Run(runner plugins.OutputRunner) error {
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
		err = coll.Insert(pack.Msg.Data)
		if err != nil {
			self.FailedCount++
			log.Println("insert failed, count=", self.FailedCount, "err:", err)
			pack.Recycle()
			continue
		}

		pack.Recycle()
	}

	return nil
}

func init() {
	plugins.RegisterOutput("MongodbOutput", func() interface{} {
		return new(MongodbOutput)
	})
}
