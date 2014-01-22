package cluster

type MessagePortManager interface {
	Register(key string) MessagePort
	GetMessagePort(key string) MessagePort
}
