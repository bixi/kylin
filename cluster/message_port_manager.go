package cluster

import (
	"errors"
	"sync"
)

type MessagePortManager interface {
	//Thread safety of callbacks of all async mthods must be guaranteed.
	RegisterAsync(key string, callbak func(MessagePort))
	GetMessagePortAsync(key string, callback func(MessagePort))
	GetMessagePortsAsync(key string, callback func([]MessagePort))
	SendTo(key string, message interface{})
	Start() error
	Stop() error
	WaitForDone()
}

type messagePortManager struct {
	sync.Mutex
	nodePorts   map[string]map[string]MessagePort
	node        Node
	commandChan chan func()
	stopChan    chan struct{}
	isStarted   bool
}

func NewMessagePortManager(node Node) MessagePortManager {
	mpm := &messagePortManager{}
	mpm.node = node
	mpm.nodePorts = make(map[string]map[string]MessagePort)
	mpm.commandChan = make(chan func(), 128)
	return mpm
}

func (mpm *messagePortManager) Start() error {
	mpm.Lock()
	defer mpm.Unlock()
	if mpm.isStarted {
		return errors.New("Already started.")
	}
	mpm.isStarted = true
	go mpm.loop()
	return nil
}

func (mpm *messagePortManager) Stop() error {
	defer recover()
	close(mpm.commandChan)
	return nil
}

func (mpm *messagePortManager) WaitForDone() {

}

func (mpm *messagePortManager) loop() {
	for command := range mpm.commandChan {
		command()
	}
}

func (mpm *messagePortManager) RegisterAsync(key string, callback func(MessagePort)) {
	command := func() {
		nodeAddress := mpm.node.Info().Address
		ports := mpm.getOrCreatePortsMap(nodeAddress)
		mp, ok := ports[key]
		if !ok {
			mp = newMessagePort()
			ports[key] = mp
		}
		callback(mp)
	}
	mpm.commandChan <- command
}

func (mpm *messagePortManager) GetMessagePortAsync(key string, callback func(MessagePort)) {
	command := func() {
		for _, ports := range mpm.nodePorts {
			mp, ok := ports[key]
			if ok {
				callback(mp)
				return
			}
		}
	}
	mpm.commandChan <- command
}

func (mpm *messagePortManager) GetMessagePortsAsync(key string, callback func([]MessagePort)) {
	command := func() {
		results := make([]MessagePort, 0)
		for _, ports := range mpm.nodePorts {
			mp, ok := ports[key]
			if ok {
				results = append(results, mp)
			}
		}
		callback(results)
	}
	mpm.commandChan <- command
}

func (mpm *messagePortManager) getOrCreatePortsMap(nodeAddress string) map[string]MessagePort {
	ports, ok := mpm.nodePorts[nodeAddress]
	if !ok {
		ports = make(map[string]MessagePort)
		mpm.nodePorts[nodeAddress] = ports
	}
	return ports
}

func (mpm *messagePortManager) SendTo(key string, message interface{}) {
	command := func() {
		for _, ports := range mpm.nodePorts {
			mp, ok := ports[key]
			if ok {
				mp.Send(message)
			}
		}
	}
	mpm.commandChan <- command
}
