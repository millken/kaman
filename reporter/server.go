package reporter

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
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
func NewServer(addr string) *Server {
	srv := &Server{
		Address: addr,
	}
	srv.starterFunc = defaultStarter

	mux := http.NewServeMux()
	runtime := new(metricsHandler)
	mux.Handle("/runtime", runtime)

	srv.server = &http.Server{
		Addr:         srv.Address,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
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
