package cluster

import (
	"log"
	"sync"
)

const (
	NotifyPath = "kylin/cluster/notify"
	ListPath   = "kylin/cluster/list"
	NodePath   = "kylin/cluster/node"
)

type Node struct {
	config            Config
	info              NodeInfo
	nodeJoinListeners []func(NodeInfo)
	isInitialized     bool
	mutex             sync.Mutex
}

func NewNode(config Config, description string) *Node {
	var node Node
	node.config = Config
	node.info.Address = Config.Address
	node.info.Description = description
	return &node
}
