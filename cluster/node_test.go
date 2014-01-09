package cluster

import (
	"log"
	"testing"
)

func TestNode(t *testing.T) {
	config = Config{"localhost:12341", []string{"localhost:12342", "localhost:12345"}}
	description = "Comet"
	node := NewNode(config)
	onNodeJoin := func(address string) {
		log.Printf("Node Join:%v", address)
		nodeList := node.List()
		log.Printf("Node List:%v", nodeList)
	}
	node.AddNodeJoinListener(onNodeJoin)
	node.Start()
	node.Stop()
}
