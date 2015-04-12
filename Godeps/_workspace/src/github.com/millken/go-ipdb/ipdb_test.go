package ipdb

import (
	"math/rand"
	"testing"
	"time"
)

const data = "ipdb.dat"

var (
	db  *DB
	err error
)

func TestHeader(t *testing.T) {
	if db, err = Load(data); err != nil {
		t.Fatal("Init failed:", err)
	}
	t.Logf("db Header = %v", db.Head)
	ip := "152.63.123.32"
	result, err := db.Find(ip)
	if err == nil { t.Logf("find %s => %s", ip, result)}
}

func TestLookup(t *testing.T) {
	if db, err = Load(data); err != nil {
		t.Fatal("Init failed:", err)
	}
	t.Logf("db Header = %v", db.Head)
	ip := "152.63.123.32"
	result, err := db.Lookup(ip)
	if err == nil { t.Logf("lookup %s country=> %s", ip, result.Country)}
}

//-----------------------------------------------------------------------------

// Benchmark command
//	go test -bench=Find
//	BenchmarkFind 1000000       1440 ns/op
func BenchmarkFind(b *testing.B) {
	b.StopTimer()
	if db, err = Load(data); err != nil {
		b.Fatal("Init failed:", err)
	}
	rand.Seed(time.Now().UnixNano())
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		rnd_ip := rand.Uint32()
		if _, err := db.FindByUint(rnd_ip); err != nil {
			b.Fatalf("FindByUint %d[%s]: %s", rnd_ip, Long2Ip(rnd_ip).To4(), err.Error())
		}
	}
}

