package cluster

import (
	"runtime"
	"testing"
)

func TestLocalPubSub(t *testing.T) {
	runtime.GOMAXPROCS(4)
	config := Config{Address: "localhost:7003", Description: "Comet"}
	node := NewNode(config)
	if node.Start() != nil {
		t.Fail()
	}
	hello := ""
	done := make(chan bool)
	onMessage := OnPubSubMessage(func(topic string, message interface{}) {
		hello = message.(string)
		done <- true
	})
	ps := NewPubSub(node, onMessage)
	if ps.Start() != nil {
		t.Fail()
	}
	ps.Subscribe("test_topic")
	ps.Publish("test_topic", "hello")
	<-done
	if hello != "hello" {
		t.Fail()
	}
	if ps.Stop() != nil {
		t.Fail()
	}
	if node.Stop() != nil {
		t.Fail()
	}
	ps.WaitForDone()
	node.WaitForDone()
}
