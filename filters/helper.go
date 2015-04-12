package filters

import (
	"github.com/millken/go-ipdb"
	"log"
)

var IpDb *ipdb.DB

func init() {
	var err error
	IpDb, err = ipdb.Load("ipdb.dat")
	if err != nil {
		log.Println("Ipdb load err ", err)
	}
}
