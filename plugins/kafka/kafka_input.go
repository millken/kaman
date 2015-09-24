package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"git.oschina.net/millken/kaman/plugins"
	"github.com/Shopify/sarama"
	"github.com/bbangert/toml"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type KafkaInputConfig struct {
	Id                         string
	Addrs                      []string
	MetadataRetries            int    `toml:"metadata_retries"`
	WaitForElection            uint32 `toml:"wait_for_election"`
	BackgroundRefreshFrequency uint32 `toml:"background_refresh_frequency"`

	// Broker Config
	MaxOpenRequests int    `toml:"max_open_reqests"`
	DialTimeout     uint32 `toml:"dial_timeout"`
	ReadTimeout     uint32 `toml:"read_timeout"`
	WriteTimeout    uint32 `toml:"write_timeout"`

	// Consumer Config
	Topic            string
	Partition        int32
	DefaultFetchSize int32  `toml:"default_fetch_size"`
	MinFetchSize     int32  `toml:"min_fetch_size"`
	MaxMessageSize   int32  `toml:"max_message_size"`
	MaxWaitTime      uint32 `toml:"max_wait_time"`
	EventBufferSize  int    `toml:"event_buffer_size"`
	OffsetValue      int64
	OffsetMethod     string
	OffsetPath       string
}

type KafkaInput struct {
	processMessageQps      int32
	processMessageOffset   int64
	processMessageCount    int64
	processMessageFailures int64

	config             *KafkaInputConfig
	common             *plugins.PluginCommonConfig
	clientConfig       *sarama.Config
	consumer           sarama.Consumer
	checkpointFile     *os.File
	stopChan           chan bool
	checkpointFilename string
}

func (k *KafkaInput) writeCheckpoint(offset int64) (err error) {
	if k.checkpointFile == nil {
		if k.checkpointFile, err = os.OpenFile(k.checkpointFilename,
			os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			return
		}
	}
	k.checkpointFile.Seek(0, 0)
	err = binary.Write(k.checkpointFile, binary.LittleEndian, &offset)
	return
}

func (k *KafkaInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	log.Println("KafkaInput Init.")
	k.common = pcf
	hn, err := os.Hostname()
	if err != nil {
		hn = "kafkaserver"
	}
	k.config = &KafkaInputConfig{
		Id:                         hn,
		MetadataRetries:            3,
		WaitForElection:            250,
		BackgroundRefreshFrequency: 10 * 60 * 1000,
		MaxOpenRequests:            4,
		DialTimeout:                60 * 1000,
		ReadTimeout:                60 * 1000,
		WriteTimeout:               60 * 1000,
		DefaultFetchSize:           1024 * 32,
		MinFetchSize:               1,
		MaxWaitTime:                250,
		EventBufferSize:            16,
		OffsetValue:                0,
		OffsetMethod:               "Manual",
		OffsetPath:                 "/tmp/kaman",
	}
	if err := toml.PrimitiveDecode(conf, k.config); err != nil {
		return fmt.Errorf("Can't unmarshal kafka config: %s", err)
	}
	if len(k.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}
	k.clientConfig = sarama.NewConfig()
	k.clientConfig.Metadata.Retry.Max = k.config.MetadataRetries
	k.clientConfig.Metadata.Retry.Backoff = time.Duration(k.config.WaitForElection) * time.Millisecond
	k.clientConfig.Metadata.RefreshFrequency = time.Duration(k.config.BackgroundRefreshFrequency) * time.Millisecond

	k.clientConfig.Net.MaxOpenRequests = k.config.MaxOpenRequests
	k.clientConfig.Net.DialTimeout = time.Duration(k.config.DialTimeout) * time.Millisecond
	k.clientConfig.Net.ReadTimeout = time.Duration(k.config.ReadTimeout) * time.Millisecond
	k.clientConfig.Net.WriteTimeout = time.Duration(k.config.WriteTimeout) * time.Millisecond

	k.clientConfig.Consumer.Fetch.Default = k.config.DefaultFetchSize
	k.clientConfig.Consumer.Fetch.Min = k.config.MinFetchSize
	k.clientConfig.Consumer.Fetch.Max = k.config.MaxMessageSize
	k.clientConfig.Consumer.MaxWaitTime = time.Duration(k.config.MaxWaitTime) * time.Millisecond
	k.checkpointFilename = filepath.Join(k.config.OffsetPath,
		fmt.Sprintf("%s.%d.offset.bin", k.config.Topic, k.config.Partition))
	if err = os.MkdirAll(filepath.Dir(k.checkpointFilename), 0766); err != nil {
		return
	}
	switch k.config.OffsetMethod {
	case "Manual":
		if fileExists(k.checkpointFilename) {
			if k.config.OffsetValue, err = readCheckpoint(k.checkpointFilename); err != nil {
				return fmt.Errorf("readCheckpoint %s", err)
			}
		} else {
			k.config.OffsetValue = sarama.OffsetOldest
		}
	case "Newest":
		k.config.OffsetValue = sarama.OffsetNewest
		if fileExists(k.checkpointFilename) {
			if err = os.Remove(k.checkpointFilename); err != nil {
				return
			}
		}
	case "Oldest":
		k.config.OffsetValue = sarama.OffsetOldest
		if fileExists(k.checkpointFilename) {
			if err = os.Remove(k.checkpointFilename); err != nil {
				return
			}
		}
	default:
		return fmt.Errorf("invalid offset_method: %s", k.config.OffsetMethod)
	}
	k.consumer, err = sarama.NewConsumer(k.config.Addrs, k.clientConfig)
	return
}

func (k *KafkaInput) Run(runner plugins.InputRunner) (err error) {
	log.Printf("KafkaInput Run. Topic: %s, Partition: %d, OffsetValue: %d\n", k.config.Topic, k.config.Partition, k.config.OffsetValue)
	k.stopChan = make(chan bool)
	var ticker = time.Tick(time.Duration(1000) * time.Millisecond)
	consumer, err := k.consumer.ConsumePartition(k.config.Topic, k.config.Partition, k.config.OffsetValue)
	defer func() {
		k.consumer.Close()
		consumer.Close()
		if k.checkpointFile != nil {
			k.checkpointFile.Close()
		}
	}()
consumerLoop:
	for {
		select {
		case err := <-consumer.Errors():
			log.Printf("consumer loop err : %q", err)
		case message := <-consumer.Messages():
			//log.Printf("\nkey=%s value=%s\n Topic=%s\nPartition=%d\nOffset=%d\n", message.Key, message.Value, message.Topic, message.Partition, message.Offset)
			atomic.AddInt64(&k.processMessageCount, 1)
			atomic.AddInt32(&k.processMessageQps, 1)
			atomic.StoreInt64(&k.processMessageOffset, message.Offset+1)
			pack := <-runner.InChan()

			pack.MsgBytes = message.Value
			pack.Msg.Tag = k.common.Tag
			pack.Msg.Timestamp = time.Now().Unix()
			pack, err = plugins.PipeFilter(k.common.Filter, pack)
			if err != nil {
				log.Printf("filter [%s] err : %s", k.common.Filter, err)
				continue
			}
			runner.RouterChan() <- pack
		case <-ticker:
			log.Printf("consumer message qps : %d", k.processMessageQps)
			atomic.StoreInt32(&k.processMessageQps, 0)
			if err = k.writeCheckpoint(atomic.LoadInt64(&k.processMessageOffset)); err != nil {

				log.Printf("writeCheckpoint [%s] err : %s", k.common.Filter, err)
			}
		case <-k.stopChan:
			break consumerLoop
		}
	}
	return
}

func (k *KafkaInput) Stop() {
	close(k.stopChan)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
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

func init() {
	plugins.RegisterInput("KafkaInput", func() interface{} {
		return new(KafkaInput)
	})
}
