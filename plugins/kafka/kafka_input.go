package kafka

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bbangert/toml"
	"github.com/millken/kaman/metrics"
	"github.com/millken/kaman/plugins"
	"github.com/optiopay/kafka"
)

type KafkaInputConfig struct {
	ClientId      string `toml:"client_id"`
	Addrs         []string
	Partition     int32
	Topic         string
	Partitions    int32
	FlushInterval uint32 `toml:"flush_interval"`
}

type KafkaInput struct {
	common   *plugins.PluginCommonConfig
	config   *KafkaInputConfig
	broker   *kafka.Broker
	consumer kafka.Consumer
}

type stdLogger struct {
}

func (stdLogger) Debug(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}
func (stdLogger) Info(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}
func (stdLogger) Warn(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}
func (stdLogger) Error(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (self *KafkaInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	log.Println("KafkaInput Init.")
	self.common = pcf
	hn, err := os.Hostname()
	if err != nil {
		hn = "kamanclient"
	}
	self.config = &KafkaInputConfig{
		ClientId:      hn,
		Partitions:    0,
		FlushInterval: 1000,
	}
	if err = toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal KafkaInput config: %s", err)
	}
	if len(self.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}
	if len(self.config.Topic) == 0 {
		return fmt.Errorf("topic is empty")
	}

	bcf := kafka.NewBrokerConf(self.config.ClientId)
	bcf.AllowTopicCreation = false
	//bcf.Logger = &stdLogger{}

	self.broker, err = kafka.Dial(self.config.Addrs, bcf)
	if err != nil {
		return fmt.Errorf("cannot connect to kafka cluster: %s", err)
	}

	defer self.broker.Close()
	consumerconf := kafka.NewConsumerConf(self.config.Topic, self.config.Partition)
	self.consumer, err = self.broker.Consumer(consumerconf)
	if err != nil {
		return fmt.Errorf("cannot create kafka consumer for %s:%d: %s", self.config.Topic, self.config.Partition, err)
	}
	return err
}

func (self *KafkaInput) Run(runner plugins.InputRunner) (err error) {
	counter := fmt.Sprintf("Tag:%s,Type:%s", self.common.Tag, self.common.Type)
	mc := metrics.NewCounter(counter)

	for {
		msg, err := self.consumer.Consume()
		if err != nil && err != kafka.ErrNoData {
			log.Printf("Consume :%s", err)
			break
		}
		pack := <-runner.InChan()
		pack.MsgBytes = bytes.TrimSpace(msg.Value)
		pack.Msg.Tag = self.common.Tag
		pack.Msg.Timestamp = time.Now().Unix()
		mc.Add(1)
		runner.RouterChan() <- pack

	}
	return nil
}

func init() {
	plugins.RegisterInput("KafkaInput", func() interface{} {
		return new(KafkaInput)
	})
}
