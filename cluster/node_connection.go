package cluster

import (
	"code.google.com/p/go.net/websocket"
	"github.com/bixi/kylin/message"
)

type NodeConnection struct {
	info            NodeInfo
	transporter     *message.Transporter
	conn            *websocket.Conn
	messageHandlers []MessageHandler
}

type MessageHandler func(message interface{}) (handled bool)

func (nc *NodeConnection) Info() NodeInfo {
	return nc.info
}

func (nc *NodeConnection) Send(message interface{}) {
	nc.transporter.Send(message)
}

func (nc *NodeConnection) AddMessageHandler(handler MessageHandler) {
	nc.messageHandlers = append(nc.messageHandlers, handler)
}

func (nc *NodeConnection) Dispatch(message interface{}) {
	for i := 0; i < len(nc.messageHandlers); i++ {
		if nc.messageHandlers[i](message) {
			break
		}
	}
}
