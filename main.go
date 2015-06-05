package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"git.oschina.net/millken/kaman/plugins"
)

var logs *log.Logger

func main() {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(error); !ok {
				fmt.Printf("PANIC: pkg: %v %s \n", r, debug.Stack())
			}
		}
	}()
	c := flag.String("c", "gofluent.conf", "config filepath")
	p := flag.String("p", "", "write cpu profile to file")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	v := flag.String("v", "error.log", "log file path")
	flag.Parse()

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
	if *memprofile != "" {
		profileMEM(*memprofile)
	}

	if *cpuprofile != "" {
		profileCPU(*cpuprofile)
	}

	masterConf, plugConf, err := LoadConfig(*c)
	if err != nil {
		log.Fatalln("read config failed, err:", err)
	}
	pipeline := plugins.NewPipeLine()
	if err := pipeline.LoadConfig(plugConf); err != nil {
		log.Fatalln("load config failed, err:", err)
	}
	pipeline.Run()
	log.Printf("masterConfig: %v\npulgConf : %v", masterConf, plugConf)

}

func profileCPU(cpuprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		time.AfterFunc(60*time.Second, func() {
			pprof.StopCPUProfile()
			f.Close()
			log.Println("Stop profiling after 60 seconds")
		})
	}
}

func profileMEM(memprofile string) {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		time.AfterFunc(60*time.Second, func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		})
	}
}
