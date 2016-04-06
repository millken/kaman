package tcp

import "testing"

func TestTcpInput1(t *testing.T) {
	tcpInput := TcpInput{}
	conf := map[string]interface{}{"net": "udp", "address": "127.0.0.1:5334"}
	err := tcpInput.Init(nil, conf)
	if err != nil {
		t.Fatalf("got %v", err)
	}

}

func TestTcpInput2(t *testing.T) {
	tcpInput := TcpInput{}
	conf := map[string]interface{}{"address": "127.0.0.1:5334"}
	err := tcpInput.Init(nil, conf)
	if err != nil {
		t.Fatalf("got %v", err)
	}
	t.Log("ok")
}
