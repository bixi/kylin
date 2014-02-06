package message

type Sendable interface {
	Send(interface{})
}
