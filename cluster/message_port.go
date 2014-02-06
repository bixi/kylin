package cluster

import "github.com/bixi/kylin/message"

type MessagePort interface {
	Send(interface{})
}

type messagePort struct {
	sendable message.Sendable
}

func newMessagePort() MessagePort {
	mp := &messagePort{}
	return mp
}

func (mp *messagePort) Send(interface{}) {

}
