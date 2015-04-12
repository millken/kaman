package plugins

import (
	"log"
)

var filter_plugins = make(map[string]func() interface{})

func RegisterFilter(name string, filter func() interface{}) {
	if filter == nil {
		log.Fatalln("filter: Register filter is nil")
	}

	if _, ok := filter_plugins[name]; ok {
		log.Fatalln("filter: Register called twice for filter " + name)
	}
	log.Println("RegisterPlugin: ", name)

	filter_plugins[name] = filter
}

func PipeFilter(name string, pack *PipelinePack) (rpack *PipelinePack, err error) {
	if filter_plugin, ok := filter_plugins[name]; ok {

		filter := filter_plugin()

		return filter.(Filter).Run(pack)
	}
	return pack, nil
}
