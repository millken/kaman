package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
  	"net/http"
	_ "net/http/pprof"
	"github.com/VividCortex/godaemon"
	"github.com/millken/kaman/plugins"
	"github.com/millken/kaman/report"
)

var logs *log.Logger
var VERSION string = "0.4.4"
var gitVersion string
var buildDate string

func init() {
	if len(gitVersion) > 0 {
		VERSION = VERSION + "/" + gitVersion
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(error); !ok {
				fmt.Printf("PANIC: pkg: %v %s \n", r, debug.Stack())
			}
		}
	}()
	c := flag.String("c", "kaman.conf", "config filepath")
	p := flag.String("p", "", "pprof serves")
	d := flag.Bool("d", false, "as daemon")
	reportaddr := flag.String("reportaddr", "", "http report addr")
	v := flag.String("v", "error.log", "log file path")
	showVersion := flag.Bool("version", false, "Prints version")
	flag.Parse()

	if *showVersion {
		version := fmt.Sprintf("build date : %s\ngit version: %s\n", buildDate, VERSION)
		fmt.Println(version)
		return
	}

	f, err := os.OpenFile(*v, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("os.Open failed, err:", err)
	}
	defer f.Close()

	w := io.MultiWriter(f, os.Stdout)
	logs = log.New(w, "", log.Ldate|log.Ltime|log.Lshortfile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(w)

	if *p != "" {
		go func() {
			log.Println(http.ListenAndServe("0.0.0.0:"+*p, nil))
		}()
	}

	if *reportaddr != "" {
		go func() {
			reporter := report.NewServer(*reportaddr)
			if err := reporter.Run(); err != nil {
				log.Fatalln("report run err:", err)
			}
		}()
	}

	masterConf, plugConf, err := LoadConfig(*c)
	if err != nil {
		log.Fatalln("read config failed, err:", err)
	}
	log.Printf("masterConfig: %v\npulgConf : %v", masterConf, plugConf)
	pipeline := plugins.NewPipeLine()
	if err := pipeline.LoadConfig(plugConf); err != nil {
		log.Fatalln("load config failed, err:", err)
	}
	plugMasterConf := plugins.DefaultMasterConfig()
	if *d {
		log.Println("as daemon run")
		godaemon.Daemonize()
	} 
	pipeline.Run(plugMasterConf)


}
