package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bbangert/toml"
	"github.com/codahale/metrics"
	"github.com/millken/kaman/plugins"
)

// Input plugin implementation that listens for Heka proself.col messages on a
// specified TCP sockeself.g Creates a separate goroutine for each TCP connection.
type TcpInput struct {
	keepAliveDuration time.Duration
	listener          net.Listener
	wg                sync.WaitGroup
	stopChan          chan bool
	config            *TcpInputConfig
	common            *plugins.PluginCommonConfig
	runner            plugins.InputRunner
}

type TcpInputConfig struct {
	// Network type (e.g. "tcp", "tcp4", "tcp6", "unix" or "unixpacket").
	// Needs to match the input type.
	Net string
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:5565").
	Address string
	// Set to true if TCP Keep Alive should be used.
	KeepAlive bool `toml:"keep_alive"`
	// Integer indicating seconds between keep alives.
	KeepAlivePeriod int `toml:"keep_alive_period"`
}

func (self *TcpInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {

	log.Println("TcpInput Init")
	self.common = pcf
	self.config = &TcpInputConfig{
		Net: "tcp",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal TcpInput config: %s", err)
	}
	address, err := net.ResolveTCPAddr(self.config.Net, self.config.Address)
	if err != nil {
		return fmt.Errorf("ResolveTCPAddress failed: %s\n", err.Error())
	}
	self.listener, err = net.ListenTCP(self.config.Net, address)
	if err != nil {
		return fmt.Errorf("ListenTCP failed: %s\n", err.Error())
	}
	// We're already listening, make sure we clean up if init fails later on.
	closeIt := true
	defer func() {
		if closeIt {
			self.listener.Close()
		}
	}()
	if self.config.KeepAlivePeriod != 0 {
		self.keepAliveDuration = time.Duration(self.config.KeepAlivePeriod) * time.Second
	}
	self.stopChan = make(chan bool)
	closeIt = false
	return nil
}

// Listen on the provided TCP connection, extracting messages from the incoming
// data until the connection is closed or Stop is called on the input.
func (self *TcpInput) handleConnection(conn net.Conn) {
	raddr := conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(raddr)
	counter := fmt.Sprintf("Tag:%s,Type:%s", self.common.Tag, self.common.Type)
	//metrics.Counter(counter).Add()
	if err != nil {
		host = raddr
	}
	log.Printf("handle conn: %s, host: %s", raddr, host)
	defer func() {
		conn.Close()
		self.wg.Done()
	}()

	count := 0
	//	tmp_count := 0
	//qc := 0
	stopped := false
	reader := bufio.NewReader(conn)
	ticker := time.Tick(time.Duration(1) * time.Second)
	for !stopped {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-self.stopChan:
			stopped = true
		case <-ticker:
			metrics.Counter(counter).AddN(uint64(count))
			metrics.Counter(counter + ":qps").SetFunc(func() uint64 {
				return uint64(count)
			})
			count = 0
			//qc = count - tmp_count
			//tmp_count = count
			//log.Printf("receive %s record: %d, qps: %d", raddr, count, qc/5)
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
					// keep the connection open, we are just checking to see if
				} else {
					log.Printf("disconnect : %s", raddr)
					stopped = true

				}
			}
			if len(line) == 0 {
				continue
			} else {
				pack := <-self.runner.InChan()
				pack.MsgBytes = bytes.TrimSpace(line)
				pack.Msg.Tag = self.common.Tag
				pack.Msg.Timestamp = time.Now().Unix()
				count++
				self.runner.RouterChan() <- pack
			}
		}
	}
}

func (self *TcpInput) Run(runner plugins.InputRunner) error {
	var (
		conn net.Conn
		e    error
	)
	self.runner = runner
	for {
		if conn, e = self.listener.Accept(); e != nil {
			if netErr, ok := e.(net.Error); ok && netErr.Temporary() {
				log.Print(fmt.Errorf("TCP accept failed: %s", e))
				continue
			} else {
				break
			}
		}
		if self.config.KeepAlive {
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				return errors.New("KeepAlive only supported for TCP Connections.")
			}
			tcpConn.SetKeepAlive(self.config.KeepAlive)
			if self.keepAliveDuration != 0 {
				tcpConn.SetKeepAlivePeriod(self.keepAliveDuration)
			}
		}
		self.wg.Add(1)
		go self.handleConnection(conn)
	}
	self.wg.Wait()
	return e
}

func init() {
	plugins.RegisterInput("TcpInput", func() interface{} {
		return new(TcpInput)
	})
}
