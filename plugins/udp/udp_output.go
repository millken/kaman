package udp

import (
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"

	"git.oschina.net/millken/kaman/plugins"

	"github.com/bbangert/toml"
)

// This is our plugin's config struct
type UdpOutputConfig struct {
	// Network type ("udp", "udp4", "udp6", or "unixgram"). Needs to match the
	// input type.
	Net string
	// String representation of the address of the network connection to which
	// we will be sending out packets (e.g. "192.168.64.48:3336").
	Address string
	// Optional address to use as the local address for the connection.
	LocalAddress string `toml:"local_address"`
	// Maximum size of message, plugin drops the data if it exceeds this limit.
	MaxMessageSize int `toml:"max_message_size"`
}

type UdpOutput struct {
	config *UdpOutputConfig
	conn   net.Conn
}

func (self *UdpOutput) Init(conf toml.Primitive) error {
	var err error
	log.Println("UdpOutput Init.")
	self.config = &UdpOutputConfig{
		Net: "udp",
		// Defines maximum size of udp data for IPv4.
		MaxMessageSize: 65507,
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal UdpOutput config: %s", err)
	}
	if self.config.MaxMessageSize < 512 {
		return fmt.Errorf("Maximum message size can't be smaller than 512 bytes.")
	}
	if self.config.Net == "unixgram" {
		if runtime.GOOS == "windows" {
			return errors.New("Can't use Unix datagram sockets on Windows.")
		}
		var unixAddr, lAddr *net.UnixAddr
		unixAddr, err = net.ResolveUnixAddr(self.config.Net, self.config.Address)
		if err != nil {
			return fmt.Errorf("Error resolving unixgram address '%s': %s", self.config.Address,
				err.Error())
		}
		if self.config.LocalAddress != "" {
			lAddr, err = net.ResolveUnixAddr(self.config.Net, self.config.LocalAddress)
			if err != nil {
				return fmt.Errorf("Error resolving local unixgram address '%s': %s",
					self.config.LocalAddress, err.Error())
			}
		}
		if self.conn, err = net.DialUnix(self.config.Net, lAddr, unixAddr); err != nil {
			return fmt.Errorf("Can't connect to '%s': %s", self.config.Address,
				err.Error())
		}
	} else {
		var udpAddr, lAddr *net.UDPAddr
		if udpAddr, err = net.ResolveUDPAddr(self.config.Net, self.config.Address); err != nil {
			return fmt.Errorf("Error resolving UDP address '%s': %s", self.config.Address,
				err.Error())
		}
		if self.config.LocalAddress != "" {
			lAddr, err = net.ResolveUDPAddr(self.config.Net, self.config.LocalAddress)
			if err != nil {
				return fmt.Errorf("Error resolving local UDP address '%s': %s",
					self.config.Address, err.Error())
			}
		}
		if self.conn, err = net.DialUDP(self.config.Net, lAddr, udpAddr); err != nil {
			return fmt.Errorf("Can't connect to '%s': %s", self.config.Address,
				err.Error())
		}
	}

	return err
}

func (self *UdpOutput) Run(runner plugins.OutputRunner) error {
	var (
		outBytes []byte
		e        error
	)
	for pack := range runner.InChan() {
		outBytes = pack.MsgBytes
		msgSize := len(outBytes)
		if msgSize > self.config.MaxMessageSize {
			e = fmt.Errorf("Message has exceeded allowed UDP data size: %d > %d",
				msgSize, self.config.MaxMessageSize)
		} else {
			self.conn.Write(outBytes)
		}
		pack.Recycle()
	}
	return e
}

func init() {
	plugins.RegisterOutput("UdpOutput", func() interface{} {
		return new(UdpOutput)
	})
}
