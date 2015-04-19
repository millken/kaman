package http

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
)

type HttpListenInput struct {
	config      *HttpListenInputConfig
	common      *plugins.PluginCommonConfig
	listener    net.Listener
	ir          plugins.InputRunner
	stopChan    chan bool
	server      *http.Server
	starterFunc func(hli *HttpListenInput) error
}

// HTTP Listen Input config struct
type HttpListenInputConfig struct {
	// TCP Address to listen to for incoming requests.
	// Defaults to "127.0.0.1:8325".
	Address        string
	Headers        http.Header
	RequestHeaders []string `toml:"request_headers"`
}

func defaultStarter(hli *HttpListenInput) (err error) {
	hli.listener, err = net.Listen("tcp", hli.config.Address)
	if err != nil {
		return fmt.Errorf("Listener [%s] start fail: %s",
			hli.config.Address, err.Error())
	} else {
		log.Printf(fmt.Sprintf("Listening on %s",
			hli.config.Address))
	}

	err = hli.server.Serve(hli.listener)
	if err != nil {
		return fmt.Errorf("Serve fail: %s", err.Error())
	}

	return nil
}

func (hli *HttpListenInput) RequestHandler(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("req.Body ReadAll failed, err : %s", err)
	}
	pack := <-hli.ir.InChan()
	pack.MsgBytes = body
	pack.Msg.Tag = hli.common.Tag
	pack.Msg.Timestamp = time.Now().Unix()
	pack, err = plugins.PipeFilter(hli.common.Filter, pack)
	if err != nil {
		log.Printf("filter [%s] err : %s", hli.common.Filter, err)
	}
	hli.ir.RouterChan() <- pack
	//log.Printf("%s, %s", req.RemoteAddr, string(body))
	//w.Write([]byte("ok"))

}

func (hli *HttpListenInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	log.Println("HttpListenInput Init.")
	hli.common = pcf
	hli.config = &HttpListenInputConfig{
		Address:        "127.0.0.1:8325",
		Headers:        make(http.Header),
		RequestHeaders: []string{},
	}

	if err := toml.PrimitiveDecode(conf, hli.config); err != nil {
		return fmt.Errorf("Can't unmarshal HttpListenInput config: %s", err)
	}
	if hli.starterFunc == nil {
		hli.starterFunc = defaultStarter
	}
	hli.stopChan = make(chan bool, 1)

	handler := http.HandlerFunc(hli.RequestHandler)
	hli.server = &http.Server{
		Handler: CustomHeadersHandler(handler, hli.config.Headers),
	}
	return nil
}

func (hli *HttpListenInput) Run(runner plugins.InputRunner) (err error) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalln("recover panic at err:", err)
		}
	}()
	hli.ir = runner
	err = hli.starterFunc(hli)
	if err != nil {
		return err
	}

	<-hli.stopChan

	return nil
}

func (hli *HttpListenInput) Stop() {
	if hli.listener != nil {
		hli.listener.Close()
	}
	close(hli.stopChan)
}

func init() {
	plugins.RegisterInput("HttpListenInput", func() interface{} {
		return new(HttpListenInput)
	})
}
