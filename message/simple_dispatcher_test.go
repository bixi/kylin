package message

import (
	"testing"
)

type DispatcherTestMessage struct {
	Value int
}

type DispatcherTestContex struct {
	Value int
}

func (contex *DispatcherTestContex) Handle(message *DispatcherTestMessage) error {
	contex.Value = message.Value
	return nil
}

func TestSimpleDispatcher(t *testing.T) {

	c := DispatcherTestContex{0}
	d := NewSimpleDispatcher(&c, false)

	m := DispatcherTestMessage{999}
	d.Dispatch(&m)
	if c.Value != 999 {
		t.Fatal("Dispatcher test failed.	")
	}
}
