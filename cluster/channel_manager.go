package cluster

type ChannelManager interface {
	Register(key string) Channel
	GetChannel(key string) Channel
}
