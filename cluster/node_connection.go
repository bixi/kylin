package cluster

import (
	"code.google.com/p/go.net/websocket"
	"github.com/bixi/kylin/message"
)

type NodeConnection struct {
	info        NodeInfo
	transporter *message.Transporter
	conn        *websocket.Conn
}

func (nc *NodeConnection) Info() NodeInfo {
	return nc.info
}
