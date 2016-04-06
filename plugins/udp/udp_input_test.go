package udp

import "testing"

func TestUdpInput1(t *testing.T) {
	udpInput := UdpInput{}
	conf := map[string]interface{}{"net": "tcp", "address": "127.0.0.1:5334"}
	err := udpInput.Init(nil, conf)
	if err != nil {
		t.Fatalf("got %v", err)
	}

}

func TestUdpInput2(t *testing.T) {
	udpInput := UdpInput{}
	conf := map[string]interface{}{"address": "127.0.0.1:5334"}
	err := udpInput.Init(nil, conf)
	if err != nil {
		t.Fatalf("got %v", err)
	}
	t.Log("ok")
}
