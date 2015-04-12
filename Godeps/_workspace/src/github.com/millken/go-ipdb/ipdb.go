package ipdb

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"net"
	"strings"
	//"fmt"
)

type Header struct {
	version uint32 // version number
	rec_len uint32 // records number
	dc_len  uint16 //datacenter number
}

type Record struct {
	ip   uint32 //ip
	mask uint8  //mask
	id   uint16 //index number
}

type Datacenter struct {
	id   uint16
	name string
}

type DB struct {
	Head Header
	Dc   map[uint16]string
	Data []byte
}

type Base struct {
	Country, City, Isp string
}

func Load(dataFile string) (db *DB, err error) {
	data, err := ioutil.ReadFile(dataFile)
	if err != nil {
		return
	}
	db = new(DB)
	db.Init(data)
	return
}

func Ip2long(ipv4 string) (ret uint32, err error) {
	ret = 0
	ip := net.ParseIP(ipv4)
	if ip == nil {
		err = errors.New("Invalid ip format")
		return
	}
	return binary.BigEndian.Uint32(ip.To4()), nil
}

func Long2Ip(a uint32) net.IP {
	return net.IPv4(byte(a>>24), byte(a>>16), byte(a>>8), byte(a))
}

func (db *DB) Init(data []byte) {
	db.Head = Header{
		binary.BigEndian.Uint32(data[:4]),
		binary.BigEndian.Uint32(data[4 : 4+4]),
		binary.BigEndian.Uint16(data[8 : 8+2]),
	}
	//fmt.Printf("head = %v", db.Head)
	db.Dc = make(map[uint16]string, db.Head.dc_len)
	for i := 0; i < int(db.Head.dc_len); i++ {
		dc_pos := 10 + int(db.Head.rec_len)*7 + i*34
		dc_pack := data[dc_pos : dc_pos+34]
		dc := Datacenter{
			binary.BigEndian.Uint16(dc_pack[:2]),
			string(dc_pack[2 : 2+32]),
		}
		db.Dc[dc.id] = dc.name
		//fmt.Printf("dc = %v", dc)
	}
	db.Data = data
	return
}

func (db *DB) Find(ipv4 string) (result string, err error) {
	var ip32 uint32
	if ip32, err = Ip2long(ipv4); err != nil {
		return "", err
	}
	return db.FindByUint(ip32)
}

func (db *DB) Lookup(ipv4 string) (*Base, error) {
	result , err := db.Find(ipv4)
	if err != nil {
		return nil, err
	}
	ip := strings.SplitN(result, "\t", 3)
	return &Base{ip[0], strings.TrimSpace(ip[1]), strings.Trim(ip[2], "\u0000")}, nil

}

func (db *DB) FindByUint(ip32 uint32) (result string, err error) {
	f := 0
	n := 0
	l := int(db.Head.rec_len) - 1
	for f <= l {
		m := int((f + l) / 2)
		n = n + 1
		p := 10 + m*7
		rec_pack := db.Data[p : p+7]
		record := Record{
			binary.BigEndian.Uint32(rec_pack[:4]),
			uint8(rec_pack[4 : 4+1][0]),
			binary.BigEndian.Uint16(rec_pack[5 : 5+2]),
		}
		rs := record.ip
		re := rs + uint32(math.Pow(2, float64(32-record.mask))) - 1
		if ip32 >= rs && ip32 <= re {
			return db.Dc[record.id], nil
		}
		if ip32 > re {f = m + 1}
		if ip32 < rs {l = m - 1}

		//fmt.Printf(" (ip32)=%d[%s], (rs)=%d[%s], (re)=%d[%s], (mask)=%d\n", ip32, Long2Ip(ip32).To4(), rs, Long2Ip(rs).To4(), re, Long2Ip(re).To4(), record.mask)

	}
	return "", errors.New("Unknown error")
}
