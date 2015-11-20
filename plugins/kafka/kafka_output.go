package kafka

import (
	"errors"
	"fmt"
	"log"
	"os"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type KafkaOutputConfig struct {
	ClientId  string `toml:"client_id"`
	Addrs     []string
	Partition int32
	Topic     string
}

type KafkaOutput struct {
	config       *KafkaOutputConfig
	broker       kafka.Client
	producerConf kafka.ProducerConf
}

func (self *KafkaOutput) Init(conf toml.Primitive) error {
	var err error
	log.Println("KafkaOutput Init.")
	hn, err := os.Hostname()
	if err != nil {
		hn = "kamanclient"
	}
	self.config = &KafkaOutputConfig{
		ClientId: hn,
	}
	if err = toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal KafkaOutput config: %s", err)
	}
	if len(self.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}
	if len(self.config.Topic) == 0 {
		return fmt.Errorf("invalid topic_variable: %s", self.config.Topic)
	}
	bcf := kafka.NewBrokerConf(self.config.ClientId)
	bcf.AllowTopicCreation = true
	// connect to kafka cluster
	self.broker, err = kafka.Dial(self.config.Addrs, bcf)
	if err != nil {
		return fmt.Errorf("cannot connect to kafka cluster: %s", err)
	}
	pf := kafka.NewProducerConf()
	self.producerConf = pf
	defer self.broker.Close()
	return err
}

func (self *KafkaOutput) Run(runner plugins.OutputRunner) error {
	producer := self.broker.Producer(self.producerConf)
	for {
		pack := <-runner.InChan()
		msg := &proto.Message{Value: pack.MsgBytes}
		if _, err := producer.Produce(self.config.Topic, self.config.Partition, msg); err != nil {
			log.Fatalf("cannot produce message to %s:%d: %s", self.config.Topic, self.config.Partition, err)
		}
		pack.Recycle()
	}
	return nil
}

func init() {
	plugins.RegisterOutput("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
