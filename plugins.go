package main

import (
	_ "github.com/millken/kaman/decoders"
	_ "github.com/millken/kaman/encoders"
	_ "github.com/millken/kaman/plugins/file"
	_ "github.com/millken/kaman/plugins/http"
	_ "github.com/millken/kaman/plugins/kafka"
	_ "github.com/millken/kaman/plugins/mongodb"
	_ "github.com/millken/kaman/plugins/tcp"
	_ "github.com/millken/kaman/plugins/udp"
)
