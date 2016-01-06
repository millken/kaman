package report

import (
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/websocket"
)

type Server struct {
	Address     string
	server      *http.Server
	listener    net.Listener
	handler     http.Handler
	starterFunc func(srv *Server) error
}

func defaultStarter(srv *Server) (err error) {
	srv.listener, err = net.Listen("tcp", srv.Address)
	if err != nil {
		return fmt.Errorf("Listener [%s] start fail: %s",
			srv.Address, err.Error())
	} else {
		log.Printf(fmt.Sprintf("Listening on %s",
			srv.Address))
	}

	err = srv.server.Serve(srv.listener)
	if err != nil {
		return fmt.Errorf("Serve fail: %s", err.Error())
	}

	return nil
}

func wsServer(ws *websocket.Conn) {
	var buf string
	defer func() {
		if err := ws.Close(); err != nil {
			log.Println("Websocket could not be closed", err.Error())
		} else {
			log.Println("Websocket closed")
		}
	}()
	//q := ws.Request().URL.Query()
	//name := q.Get("name")
	stopped := false
	ticker := time.Tick(time.Duration(1) * time.Second)
	for !stopped {
		select {
		case <-ticker:
			val := expvar.Get(metricsVar)
			if val == nil {
				buf = ""
			} else {
				buf = val.String()
			}
			_, err := ws.Write([]byte(buf))
			if err != nil {
				log.Printf("Websocket error: %s\n", err.Error())
				stopped = true
			}

		}
	}
}

func NewServer(addr string) *Server {
	srv := &Server{
		Address: addr,
	}
	srv.starterFunc = defaultStarter

	mux := http.NewServeMux()
	runtime := NewMetric()
	mux.Handle("/runtime", runtime)
	mux.Handle("/ws", websocket.Handler(wsServer))

	srv.server = &http.Server{
		Addr:    srv.Address,
		Handler: mux,
		//ReadTimeout:  10 * time.Second,
		//WriteTimeout: 10 * time.Second,
	}
	return srv
}

func (srv *Server) Run() error {
	err := srv.starterFunc(srv)
	if err != nil {
		return err
	}
	return nil
}
