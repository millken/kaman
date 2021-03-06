package plugins

import (
	"log"
)

var output_plugins = make(map[string]func() interface{})

func RegisterOutput(name string, out func() interface{}) {
	if out == nil {
		log.Fatalln("output: Register output is nil")
	}

	if _, ok := output_plugins[name]; ok {
		log.Fatalln("output: Register called twice for output " + name)
	}
	log.Println("RegisterPlugin: ", name)

	output_plugins[name] = out
}
