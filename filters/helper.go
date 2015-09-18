package filters

import (
	"github.com/millken/go-ipdb"
	"log"
	"strings"
)

var IpDb *ipdb.DB

func init() {
	var err error
	IpDb, err = ipdb.Load("ipdb.dat")
	if err != nil {
		log.Println("Ipdb load err ", err)
	}
}

func GetPrimaryDomain(name string) string {
	names := strings.Split(strings.ToLower(name), ".")
	names_len := len(names)
	if names_len <= 1 {
		return name
	}
	if names_len > 2 && strings.Contains("ac.cn|com.cn|com.au|com.sg|net.cn|gov.cn|org.cn|edu.cn|com.hk|net.hk|org.hk|net.in|co.uk",
		strings.Join(names[names_len-2:], ".")) {
		return strings.Join(names[names_len-3:], ".")
	}
	return strings.Join(names[names_len-2:], ".")
}
