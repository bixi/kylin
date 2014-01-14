package cluster

import (
	"code.google.com/p/go.net/websocket"
	"github.com/bixi/kylin/message"
	"sync"
)

type NodeConnection interface {
	Info() NodeInfo
	Start() error
	Stop() error
	AddMessageHandler(MessageHandler)
	Send(message interface{})
	Dispatch(message interface{})
	Conn() *websocket.Conn
	WaitForDone()
}

type nodeConnection struct {
	info            NodeInfo
	transporter     message.Transporter
	conn            *websocket.Conn
	messageHandlers []MessageHandler
	mutex           sync.Mutex
}

type MessageHandler func(message interface{}) (handled bool)

func newNodeConnection(conn *websocket.Conn, info NodeInfo) NodeConnection {
	nc := &nodeConnection{}
	nc.info = info
	nc.conn = conn
	handler := newNodeHandler(nc)
	nc.transporter = message.NewTransporter(handler, handler, handler, handler)
	return nc
}

func (nc *nodeConnection) Conn() *websocket.Conn {
	return nc.conn
}
func (nc *nodeConnection) Info() NodeInfo {
	return nc.info
}

func (nc *nodeConnection) Start() error {
	return nc.transporter.Start()
}

func (nc *nodeConnection) Stop() error {
	return nc.transporter.Stop()
}

func (nc *nodeConnection) Send(message interface{}) {
	nc.transporter.Send(message)
}

func (nc *nodeConnection) AddMessageHandler(handler MessageHandler) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.messageHandlers = append(nc.messageHandlers, handler)
}

func (nc *nodeConnection) Dispatch(message interface{}) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	for i := 0; i < len(nc.messageHandlers); i++ {
		if nc.messageHandlers[i](message) {
			break
		}
	}
}

func (nc *nodeConnection) WaitForDone() {
	nc.transporter.WaitForDone()
}
