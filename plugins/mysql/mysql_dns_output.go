package mysql

import (
	//"encoding/json"
	"database/sql"
	"fmt"
	"log"
	"time"

	"git.oschina.net/millken/kaman/plugins"
	"github.com/bbangert/toml"
	_ "github.com/go-sql-driver/mysql"
)

type DnsQueryStats struct {
	Server, Domain, Month, Day, Hour string //server,domain
	//DateTime                         time.Time
	//Nums int64
}

type MysqlOutputConfig struct {
	Host, Port, Database string
	User, Password       string
}

type MysqlDnsQueryOutput struct {
	config      *MysqlOutputConfig
	FailedCount int64
	outputExit  chan error
}

func (self *MysqlDnsQueryOutput) Init(conf toml.Primitive) error {
	log.Println("MysqlDnsQueryOutput Init.")
	self.config = &MysqlOutputConfig{
		Host:     "localhost",
		Port:     "3306",
		Database: "test",
		User:     "root",
		Password: "",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal MysqlDnsQueryOutput config: %s", err)
	}
	self.outputExit = make(chan error)
	return nil
}

func (self *MysqlDnsQueryOutput) Run(runner plugins.OutputRunner) error {
	var (
		ok        = true
		outBatch  = make(map[DnsQueryStats]int)
		batchData = make(map[DnsQueryStats]int)
		ticker    = time.Tick(time.Duration(5) * time.Second)
		nums      = 0
	)

	//yd:123456@tcp(192.168.3.104:3306)/db?charset=utf8&autocommit=true
	url := ""
	if len(self.config.User) != 0 && len(self.config.Password) != 0 {
		url += self.config.User + ":" + self.config.Password + "@"
	}
	url += "tcp(" + self.config.Host + ":" + self.config.Port + ")/" + self.config.Database
	dbs, err := sql.Open("mysql", url)
	if err != nil {
		log.Println("connect mysql failed, err:", err)
		return err
	}
	defer dbs.Close()

	for ok {
		select {
		case pack := <-runner.InChan():
			dnsquery := DnsQueryStats{
				Server: pack.Msg.Data["serverip"].(string),
				Domain: pack.Msg.Data["primarydomain"].(string),
				Month:  pack.Msg.Data["month"].(string),
				Day:    pack.Msg.Data["day"].(string),
				Hour:   pack.Msg.Data["hour"].(string),
			}
			if num := outBatch[dnsquery]; num > 0 {
				outBatch[dnsquery] = num + 1
			} else {
				outBatch[dnsquery] = 1
			}
			/*
				server := pack.Msg.Data["serverip"].(string)
				domain := pack.Msg.Data["primarydomain"].(string)
				month := pack.Msg.Data["month"].(string)
				day := pack.Msg.Data["day"].(string)
				hour := pack.Msg.Data["hour"].(string)

				err := dbs.QueryRow("SELECT nums FROM query WHERE ip=? and domain=? and month=? and day=? and hour=?",
					server, domain, month, day, hour).Scan(&nums)
				if err != nil {
					sqlstr := fmt.Sprintf("insert into `query` (ip,domain,nums,month,day,hour) values ('%s','%s', 1, '%s','%s','%s')",
						server, domain, month, day, hour)
					//fmt.Println(sqlstr)
					_, err := dbs.Exec(sqlstr)
					if err != nil {
						log.Println("query mysql failed, err:", err)
					}
				} else {
					sqlstr := fmt.Sprintf("update query set nums=nums+1 where ip='%s' and domain='%s' and month='%s' and day='%s' and hour='%s'",
						server, domain, month, day, hour)
					//fmt.Println(sqlstr)
					_, err := dbs.Exec(sqlstr)
					if err != nil {
						log.Println("query mysql failed, err:", err)
					}
				}
			*/
			pack.Recycle()
		case <-ticker:
			dbs.Ping()
			batchData = outBatch
			outBatch = make(map[DnsQueryStats]int)
			for q, n := range batchData {
				err := dbs.QueryRow("SELECT nums FROM query WHERE ip=? and domain=? and month=? and day=? and hour=?",
					q.Server, q.Domain, n, q.Month, q.Day, q.Hour).Scan(&nums)
				if err != nil {
					sqlstr := fmt.Sprintf("insert into `query` (ip,domain,nums,month,day,hour) values ('%s','%s', '%d', '%s','%s','%s')",
						q.Server, q.Domain, n, q.Month, q.Day, q.Hour)
					//fmt.Println(sqlstr)
					_, err := dbs.Exec(sqlstr)
					if err != nil {
						log.Println("query mysql failed, err:", err)
					}
				} else {
					sqlstr := fmt.Sprintf("update query set nums=nums+%d where ip='%s' and domain='%s' and month='%s' and day='%s' and hour='%s'",
						n, q.Server, q.Domain, n, q.Month, q.Day, q.Hour)
					//fmt.Println(sqlstr)
					_, err := dbs.Exec(sqlstr)
					if err != nil {
						log.Println("query mysql failed, err:", err)
					}
				}
			}
		case err = <-self.outputExit:
			ok = false

		}
	}

	return nil
}

func init() {
	plugins.RegisterOutput("MysqlDnsQueryOutput", func() interface{} {
		return new(MysqlDnsQueryOutput)
	})
}
