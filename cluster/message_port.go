package cluster

type MessagePort interface {
	Key() string
	Send(interface{})
	GlobalData() interface{}
	LocalData() interface{}
	SetLocalData(interface{})
	SetGlobalData(interface{})
	SetOnGlobalDataChange(func(interface{}))
	Subscribe(string)
	Owner() NodeInfo
	IsOwned() bool
}
