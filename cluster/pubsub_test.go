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
	c := make(chan []string)
	onMessage := OnPubSubMessage(func(topic string, message interface{}) {
		hello = message.(string)
		done <- true
	})
	ps := NewPubSub(node, onMessage)
	if ps.Start() != nil {
		t.Fail()
	}
	topic := "test_topic"
	ps.Subscribe(topic)
	ps.GetSubscribersAsync(topic, c)
	result := <-c
	if result[0] != node.Info().Address {
		t.Fail()
	}

	ps.Publish(topic, "hello")
	<-done
	if hello != "hello" {
		t.Fail()
	}

	ps.Unsubscribe(topic)
	ps.GetSubscribersAsync(topic, c)
	result = <-c
	if result != nil {
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

func TestRemotePubSub(t *testing.T) {
	runtime.GOMAXPROCS(4)
	config1 := Config{"localhost:7403", "Comet1", []string{"localhost:7404"}}
	config2 := Config{"localhost:7404", "Comet2", []string{"localhost:7403"}}
	node1 := NewNode(config1)
	node2 := NewNode(config2)

	done1 := make(chan int, 10)
	onMessage1 := OnPubSubMessage(func(topic string, message interface{}) {
		done1 <- message.(int)
	})
	ps1 := NewPubSub(node1, onMessage1)

	done2 := make(chan int, 10)
	onMessage2 := OnPubSubMessage(func(topic string, message interface{}) {
		done2 <- message.(int)
	})
	ps2 := NewPubSub(node2, onMessage2)

	node1.Start()
	node2.Start()

	ps1.Start()
	ps2.Start()

	topic := "test_topic"
	ps1.Subscribe(topic)
	ps2.Subscribe(topic)
	for {
		// wait for subscribe done
		if len(ps1.GetSubscribers(topic)) == 2 && len(ps2.GetSubscribers(topic)) == 2 {
			break
		}

	}
	ps1.Publish(topic, 42)
	if <-done1 != 42 {
		t.Fail()
	}
	if <-done2 != 42 {
		t.Fail()
	}

	ps1.Unsubscribe(topic)
	ps1.Publish(topic, 43)
	if <-done2 != 43 {
		t.Fail()
	}
	subs2 := ps2.GetSubscribers(topic)
	if len(subs2) != 1 || subs2[0] != node2.Info().Address {
		t.Fail()
	}
	ps1.Stop()
	ps2.Stop()
	node1.Stop()
	node2.Stop()
	ps1.WaitForDone()
	ps2.WaitForDone()
	node1.WaitForDone()
	node2.WaitForDone()
}
