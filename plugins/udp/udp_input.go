package udp

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/millken/kaman/metrics"
	"github.com/millken/kaman/plugins"

	"github.com/bbangert/toml"
)

const UDP_PACKET_SIZE = 4096

type UdpPack struct {
	addr   *net.UDPAddr
	buffer []byte
}
type UdpInput struct {
	listener *net.UDPConn
	stopChan chan bool
	config   *UdpInputConfig
	common   *plugins.PluginCommonConfig
	runner   plugins.InputRunner
	inChan   chan UdpPack
	buffer   bytes.Buffer
}

type UdpInputConfig struct {
	// Network type (e.g. "tcp", "tcp4", "tcp6", "unix" or "unixpacket").
	// Needs to match the input type.
	Net string
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:5565").
	Address string
}

func (self *UdpInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {

	log.Println("UdpInput Init")
	self.common = pcf
	self.config = &UdpInputConfig{
		Net: "udp4",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal UdpInput config: %s", err)
	}
	address, err := net.ResolveUDPAddr(self.config.Net, self.config.Address)
	if err != nil {
		return fmt.Errorf("ResolveUDPAddress failed: %s\n", err.Error())
	}
	self.listener, err = net.ListenUDP(self.config.Net, address)
	if err != nil {
		return fmt.Errorf("ListenUDP failed: %s\n", err.Error())
	}
	self.listener.SetReadBuffer(1048576)
	closeIt := true
	defer func() {
		if closeIt {
			self.listener.Close()
		}
	}()
	self.stopChan = make(chan bool)
	self.inChan = make(chan UdpPack, 1024)
	closeIt = false
	return nil
}

func (self *UdpInput) Run(runner plugins.InputRunner) error {
	var (
		e error
	)
	self.runner = runner
	buf := make([]byte, UDP_PACKET_SIZE)
	stopped := false
	counter := fmt.Sprintf("Tag:%s,Type:%s", self.common.Tag, self.common.Type)
	mc := metrics.NewCounter(counter)
	for !stopped {
		select {
		case <-self.stopChan:
			stopped = true
		default:
			n, _, err := self.listener.ReadFromUDP(buf)
			if err != nil {
				log.Printf("read from udp err : %s", err)
				continue
			}
			//log.Printf("get %d from %s: %s", n, addr, buf[0:n])
			self.buffer.Write(buf[:n])
			for true {
				line, err := self.buffer.ReadBytes('\n')
				if err != nil {
					break
				}

				pack := <-self.runner.InChan()
				pack.MsgBytes = bytes.TrimSpace(line)
				pack.Msg.Tag = self.common.Tag
				pack.Msg.Timestamp = time.Now().Unix()
				mc.Add(1)
				self.runner.RouterChan() <- pack
			}

		}
	}
	return e
}

func init() {
	plugins.RegisterInput("UdpInput", func() interface{} {
		return new(UdpInput)
	})
}
