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

type outBatch struct {
	data []*proto.Message
}

func newOutBatch() *outBatch {
	return &outBatch{
		data: make([]*proto.Message, 0),
	}
}

type KafkaOutputConfig struct {
	ClientId      string `toml:"client_id"`
	Addrs         []string
	Partition     int32
	Topic         string
	Partitions    int32
	Distributer   string
	FlushInterval uint32 `toml:"flush_interval"`
}

type KafkaOutput struct {
	common               *plugins.PluginCommonConfig
	config               *KafkaOutputConfig
	broker               *kafka.Broker
	producer             kafka.Producer
	distributingProducer kafka.DistributingProducer
	batchChan            chan *outBatch
	backChan             chan *outBatch
}

func (self *KafkaOutput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	log.Println("KafkaOutput Init.")
	self.common = pcf
	hn, err := os.Hostname()
	if err != nil {
		hn = "kamanclient"
	}
	self.config = &KafkaOutputConfig{
		ClientId:      hn,
		Distributer:   "None",
		Partitions:    0,
		FlushInterval: 1000,
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
	//bcf.AllowTopicCreation = true

	// connect to kafka cluster
	self.broker, err = kafka.Dial(self.config.Addrs, bcf)
	if err != nil {
		return fmt.Errorf("cannot connect to kafka cluster: %s", err)
	}

	defer self.broker.Close()
	pf := kafka.NewProducerConf()
	pf.RequiredAcks = 1
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
	self.batchChan = make(chan *outBatch)
	self.backChan = make(chan *outBatch, 2) // Never block on the hand-back
	return err
}

func (self *KafkaOutput) Run(runner plugins.OutputRunner) (err error) {
	var (
		timer         *time.Timer
		timerDuration time.Duration
		pack 		  *plugins.PipelinePack
		//message       *proto.Message
		//outMessages   []*proto.Message
	)
	errChan := make(chan error, 1)

	go self.committer(runner, errChan)

	out := newOutBatch()
	message := &proto.Message{Value: nil}
	ok := true
	var ticker = time.NewTicker(time.Duration(5) * time.Second)
	defer ticker.Stop()
	timerDuration = time.Duration(self.config.FlushInterval) * time.Millisecond
	timer = time.NewTimer(timerDuration)
	if self.distributingProducer != nil {
		for ok {
			select {
			case pack = <-runner.InChan():
				pack, err = plugins.PipeDecoder(self.common.Decoder, pack)
				if err != nil {
					log.Printf("PipeDecoder :%s", err)
					pack.Recycle()
					continue
				}
				pack, err = plugins.PipeEncoder(self.common.Encoder, pack)
				if err != nil {
					log.Printf("PipeEncoder :%s", err)
					pack.Recycle()
					continue
				}
				message = &proto.Message{Value: pack.Msg.MsgBytes}
				out.data = append(out.data, message)
				pack.Recycle()
			case <-timer.C:
				self.batchChan <- out
				out = <-self.backChan
				timer.Reset(timerDuration)
			case <-ticker.C:
				if err != nil {
					bcf := kafka.NewBrokerConf(self.config.ClientId)
					//bcf.AllowTopicCreation = true

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
			pack = <-runner.InChan()
			message = &proto.Message{Value: pack.Msg.MsgBytes}
			if _, err = self.producer.Produce(self.config.Topic, self.config.Partition, message); err != nil {
				log.Printf("cannot produce message to %s:%d: %s", self.config.Topic, self.config.Partition, err)
			}
			pack.Recycle()
		}
	}
	return nil
}

func (self *KafkaOutput) committer(or plugins.OutputRunner, errChan chan error) {
	initBatch := newOutBatch()
	self.backChan <- initBatch
	var out *outBatch
	var err error
	ok := true
	for ok {
		select {
		case out, ok = <-self.batchChan:
			if !ok {
				log.Printf("batchChan are not ready")
				continue
			}
			//log.Printf("out=%#v", out)
			if _, err = self.distributingProducer.Distribute(self.config.Topic, out.data...); err != nil {
				log.Printf("cannot produce message to %s: %s", self.config.Topic, err)
			}

			out.data = out.data[:0]
			self.backChan <- out
		}
	}
}
func init() {
	plugins.RegisterOutput("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
