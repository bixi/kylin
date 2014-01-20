package cluster

import (
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"github.com/bixi/kylin/message"
	"log"
	"sync"
)

type NodeConnection interface {
	Info() NodeInfo
	Start() error
	Stop() error
	AddMessageHandler(MessageHandler)
	Send(message interface{})
	Conn() *websocket.Conn
	WaitForDone()
}

type nodeConnection struct {
	sync.Mutex
	gobEncoder      *gob.Encoder
	gobDecoder      *gob.Decoder
	localNode       Node
	info            NodeInfo
	transporter     message.Transporter
	conn            *websocket.Conn
	messageHandlers []MessageHandler
}

type MessageHandler func(message interface{}) (handled bool)

func newNodeConnection(localNode Node, conn *websocket.Conn, info NodeInfo) NodeConnection {
	nc := &nodeConnection{}
	nc.info = info
	nc.conn = conn
	nc.localNode = localNode
	nc.gobEncoder = gob.NewEncoder(conn)
	nc.gobDecoder = gob.NewDecoder(conn)
	nc.transporter = message.NewTransporter(nc, nc, nc, nc)
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
	nc.Lock()
	defer nc.Unlock()
	nc.messageHandlers = append(nc.messageHandlers, handler)
}

func (nc *nodeConnection) Dispatch(message interface{}) error {
	nc.Lock()
	defer nc.Unlock()
	for i := 0; i < len(nc.messageHandlers); i++ {
		if nc.messageHandlers[i](message) {
			break
		}
	}
	return nil
}

func (nc *nodeConnection) WaitForDone() {
	nc.transporter.WaitForDone()
}

func (nc *nodeConnection) Encode(message interface{}) error {
	return nc.gobEncoder.Encode(&message)
}

func (nc *nodeConnection) Decode(message interface{}) error {
	return nc.gobDecoder.Decode(message)
}

func (nc *nodeConnection) OnError(err error) {
	log.Printf("node %v error:%v \n", nc.Conn().RemoteAddr(), err)
}
