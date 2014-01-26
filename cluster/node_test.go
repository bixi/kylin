package cluster

import (
	"runtime"
	"testing"
)

func TestNodeConfig(t *testing.T) {
	config := Config{"localhost:12341", "Comet", []string{"localhost:12342", "localhost:12345"}}
	node := NewNode(config)
	if node.Info().Address != config.Address {
		t.Fail()
	}
	if node.Info().Description != config.Description {
		t.Fail()
	}
}

func TestNodeList(t *testing.T) {
	config := Config{"localhost:12345", "Comet", nil}
	node := NewNode(config)
	node.Start()
	list := node.List()
	if len(list) != 1 {
		t.Errorf("TestNodeList:length %d, expected %d", len(list), 1)
	}
	if list[0] != node.Info() {
		t.Errorf("TestNodeList:info %q, expected %q", list[0], node.Info())
	}
	node.Stop()
	node.WaitForDone()

}

func startTestingNode(config Config, onNodeJoin NodeJoinListener, onNodeDrop NodeDropListener) (Node, error) {
	node := NewNode(config)
	if onNodeJoin != nil {
		node.AddNodeJoinListener(onNodeJoin)
	}
	if onNodeDrop != nil {
		node.AddNodeDropListener(onNodeDrop)
	}
	err := node.Start()
	return node, err
}

func TestNodeJoin(t *testing.T) {
	runtime.GOMAXPROCS(4)
	config1 := Config{"localhost:12341", "Comet1", []string{"localhost:12342"}}
	config2 := Config{"localhost:12342", "Comet2", []string{"localhost:12341"}}
	done := make(chan bool)
	var node1, node2 Node
	var result1, result2 string
	onNodeJoin1 := func(conn NodeConnection) {
		result1 = conn.Info().Description
		done <- true
	}
	onNodeJoin2 := func(conn NodeConnection) {
		result2 = conn.Info().Description
		done <- true
	}
	node1, err := startTestingNode(config1, onNodeJoin1, nil)
	if err != nil {
		t.Error(err)
	}
	node2, err = startTestingNode(config2, onNodeJoin2, nil)
	if err != nil {
		t.Error(err)
	}
	<-done
	<-done
	node1.Stop()
	node2.Stop()
	node1.WaitForDone()
	node2.WaitForDone()
	if result1 != "Comet2" {
		t.Error("TestNodeJoin failed.")
	}
	if result2 != "Comet1" {
		t.Error("TestNodeJoin failed.")
	}
}

func TestNodeDrop(t *testing.T) {
	runtime.GOMAXPROCS(4)
	config1 := Config{"localhost:12341", "Comet1", []string{"localhost:12342"}}
	config2 := Config{"localhost:12342", "Comet2", []string{"localhost:12341"}}
	done := make(chan bool)
	var node1, node2 Node
	var result1, result2 string
	onNodeJoin1 := func(conn NodeConnection) {
		conn.Stop()
	}
	onNodeJoin2 := func(conn NodeConnection) {
	}
	onNodeDrop1 := func(conn NodeConnection) {
		result1 = conn.Info().Description
		done <- true
	}
	onNodeDrop2 := func(conn NodeConnection) {
		result2 = conn.Info().Description
		done <- true
	}
	node1, err := startTestingNode(config1, onNodeJoin1, onNodeDrop1)
	if err != nil {
		t.Error(err)
	}
	node2, err = startTestingNode(config2, onNodeJoin2, onNodeDrop2)
	if err != nil {
		t.Error(err)
	}
	<-done
	<-done
	node1.Stop()
	node2.Stop()
	node1.WaitForDone()
	node2.WaitForDone()
	if result1 != "Comet2" {
		t.Error("TestNodeDrop failed.")
	}
	if result2 != "Comet1" {
		t.Error("TestNodeDrop failed.")
	}
}

func TestNodeAutoFound(t *testing.T) {
	runtime.GOMAXPROCS(4)
	config1 := Config{"localhost:12341", "Comet1", []string{}}
	config2 := Config{"localhost:12342", "Comet2", []string{"localhost:12341", "localhost:12343"}}
	config3 := Config{"localhost:12343", "Comet3", []string{}}
	done := make(chan bool)
	var node1, node2, node3 Node
	var result1, result3 string
	onNodeJoin1 := func(conn NodeConnection) {
		if conn.Info().Description == "Comet3" {
			result1 = conn.Info().Description
			done <- true
		}
	}
	onNodeJoin2 := func(conn NodeConnection) {
		done <- true
	}
	onNodeJoin3 := func(conn NodeConnection) {
		if conn.Info().Description == "Comet1" {
			result3 = conn.Info().Description
			done <- true
		}
	}
	node1, err := startTestingNode(config1, onNodeJoin1, nil)
	if err != nil {
		t.Error(err)
	}
	node2, err = startTestingNode(config2, onNodeJoin2, nil)
	if err != nil {
		t.Error(err)
	}
	node3, err = startTestingNode(config3, onNodeJoin3, nil)
	if err != nil {
		t.Error(err)
	}
	<-done
	<-done
	<-done
	<-done
	if len(node1.List()) != 3 {
		t.Error("TestNodeAutoFound failed.")
	}
	if len(node2.List()) != 3 {
		t.Error("TestNodeAutoFound failed.")
	}
	if len(node3.List()) != 3 {
		t.Error("TestNodeAutoFound failed.")
	}
	node1.Stop()
	node2.Stop()
	node3.Stop()
	node1.WaitForDone()
	node2.WaitForDone()
	node3.WaitForDone()
	if result1 != "Comet3" {
		t.Error("TestNodeAutoFound failed.")
	}
	if result3 != "Comet1" {
		t.Error("TestNodeAutoFound failed.")
	}
}
