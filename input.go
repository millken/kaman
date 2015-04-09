package main

import (
	"log"
)

var input_plugins = make(map[string]func() interface{})

func RegisterInput(name string, input func() interface{}) {
	if input == nil {
		log.Fatalln("input: Register input is nil")
	}

	if _, ok := input_plugins[name]; ok {
		log.Fatalln("input: Register called twice for input " + name)
	}

	input_plugins[name] = input
}
