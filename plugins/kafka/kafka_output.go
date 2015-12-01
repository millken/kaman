package kafka

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bbangert/toml"
	"github.com/millken/kaman/plugins"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type KafkaOutputConfig struct {
	ClientId    string `toml:"client_id"`
	Addrs       []string
	Partition   int32
	Topic       string
	Partitions  int32
	Distributer string
}

type KafkaOutput struct {
	common               *plugins.PluginCommonConfig
	config               *KafkaOutputConfig
	broker               *kafka.Broker
	producer             kafka.Producer
	distributingProducer kafka.DistributingProducer
}

func (self *KafkaOutput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	log.Println("KafkaOutput Init.")
	self.common = pcf
	hn, err := os.Hostname()
	if err != nil {
		hn = "kamanclient"
	}
	self.config = &KafkaOutputConfig{
		ClientId:    hn,
		Distributer: "None",
		Partitions:  0,
	}
	if err = toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal KafkaOutput config: %s", err)
	}
	if len(self.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}
	if len(self.config.Topic) == 0 {
		return fmt.Errorf("topic is empty")
	}

	bcf := kafka.NewBrokerConf(self.config.ClientId)
	bcf.AllowTopicCreation = true

	// connect to kafka cluster
	self.broker, err = kafka.Dial(self.config.Addrs, bcf)
	if err != nil {
		return fmt.Errorf("cannot connect to kafka cluster: %s", err)
	}

	//defer broker.Close()
	pf := kafka.NewProducerConf()
	self.producer = self.broker.Producer(pf)
	partitions, err := self.broker.PartitionCount(self.config.Topic)
	if err != nil {
		return fmt.Errorf("cannot count to topic partitions: %s", err)
	}
	log.Printf("topic\"%s\" has %d partitions\n", self.config.Topic, partitions)
	if (self.config.Partition + 1) > partitions {
		return fmt.Errorf("invalid partition: %d, topic have %d partitions",
			self.config.Partition, partitions)
	}
	if self.config.Partitions == 0 {
		self.config.Partitions = partitions
	}
	switch self.config.Distributer {
	case "Random":
		self.distributingProducer = kafka.NewRandomProducer(self.producer, self.config.Partitions)
	case "RoundRobin":
		self.distributingProducer = kafka.NewRoundRobinProducer(self.producer, self.config.Partitions)
	case "Hash":
		self.distributingProducer = kafka.NewHashProducer(self.producer, self.config.Partitions)
	case "None":
		self.distributingProducer = nil
	default:
		return fmt.Errorf("invalid distributer: %s, must be one of these: \"Random\",\"RoundRobin\",\"Hash\"", self.config.Distributer)
	}

	return err
}

func (self *KafkaOutput) Run(runner plugins.OutputRunner) (err error) {
	ok := true
	err = nil
	var ticker = time.Tick(time.Duration(5) * time.Second)
	if self.distributingProducer != nil {
		for ok {
			select {
			case pack := <-runner.InChan():
				if err == nil {
					msg := &proto.Message{Value: pack.MsgBytes}
					if _, err = self.distributingProducer.Distribute(self.config.Topic, msg); err != nil {
						log.Printf("cannot produce message to %s: %s", self.config.Topic, err)
					}
				}
				pack.Recycle()
			case <-ticker:
				if err != nil {
					bcf := kafka.NewBrokerConf(self.config.ClientId)
					bcf.AllowTopicCreation = true

					// connect to kafka cluster
					self.broker, err = kafka.Dial(self.config.Addrs, bcf)
					if err != nil {
						log.Printf("cannot reconnect to kafka cluster: %s", err)
					}
				}
			}
		}
	} else {
		for {
			pack := <-runner.InChan()
			msg := &proto.Message{Value: pack.MsgBytes}
			if _, err := self.producer.Produce(self.config.Topic, self.config.Partition, msg); err != nil {
				log.Printf("cannot produce message to %s:%d: %s", self.config.Topic, self.config.Partition, err)
			}
			pack.Recycle()
		}
	}
	return nil
}

func init() {
	plugins.RegisterOutput("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
