package cluster

import (
	"errors"
	"github.com/bixi/kylin/utility"
	"sync"
)

type PubSub interface {
	Publish(topic string, message interface{})
	Subscribe(topic string)
	Unsubscribe(topic string)
	GetSubscribersAsync(topic string, resultChan chan []string)
	GetSubscribers(topic string) []string
	Start() error
	Stop() error
	WaitForDone()
}

type OnPubSubMessage func(topic string, message interface{})

type pubsub struct {
	sync.Mutex
	node        Node
	commandChan utility.NonblockingChan
	subscribers map[string]map[string]bool // topic -> node connection addresses
	nodeTopics  map[string]map[string]bool // address -> topic set
	onMessage   OnPubSubMessage
	isStarted   bool
}

func NewPubSub(node Node, onMessage OnPubSubMessage) PubSub {
	ps := &pubsub{}
	ps.node = node
	ps.commandChan = utility.NewNonblockingChan(128)
	ps.onMessage = onMessage
	ps.subscribers = make(map[string]map[string]bool)
	ps.nodeTopics = make(map[string]map[string]bool)
	node.RegisterMessage(subscribedTopicsMessage{})
	node.RegisterMessage(subscribeMessage{})
	node.RegisterMessage(unsubscribeMessage{})
	node.RegisterMessage(publishMessage{})
	onNodeJoin := NodeJoinListener(func(nc NodeConnection) {
		nc.AddMessageHandler(MessageHandler(
			func(message interface{}) bool {
				return ps.handleMessage(nc.Info().Address, message)
			}))
		command := func() {
			ps.sendMyTopicsTo(nc)
		}
		ps.postCommand(command)
	})
	onNodeDrop := NodeDropListener(func(nc NodeConnection) {
		command := func() {
			ps.removeSubscribersByAddress(nc.Info().Address)
		}
		ps.postCommand(command)
	})
	ps.node.AddNodeJoinListener(onNodeJoin)
	ps.node.AddNodeDropListener(onNodeDrop)
	return ps
}

func (ps *pubsub) postCommand(command func()) {
	defer func() {
		recover()
	}()
	ps.commandChan.Out <- command

}

type subscribedTopicsMessage struct {
	Topics []string
}

type subscribeMessage struct {
	Topic string
}

type unsubscribeMessage struct {
	Topic string
}

type publishMessage struct {
	Topic   string
	Message interface{}
}

func (ps *pubsub) sendMyTopicsTo(nc NodeConnection) {
	topicMap, ok := ps.nodeTopics[ps.node.Info().Address]
	if ok {
		message := subscribedTopicsMessage{}
		message.Topics = make([]string, 0, len(topicMap))
		for k, _ := range topicMap {
			message.Topics = append(message.Topics, k)
		}
		nc.Send(message)
	}
}

func (ps *pubsub) removeSubscribersByAddress(address string) {
	topicMap, ok := ps.nodeTopics[address]
	if ok {
		for k, _ := range topicMap {
			ncs, ok := ps.subscribers[k]
			if ok {
				delete(ncs, address)
			}
		}
		delete(ps.nodeTopics, address)
	}
}

func (ps *pubsub) broadcast(message interface{}) {
	ps.handleMessage(ps.node.Info().Address, message)
	ps.node.Broadcast(message)
}

func (ps *pubsub) Subscribe(topic string) {
	command := func() {
		subs := ps.getOrCreateSubscribers(topic)
		_, ok := subs[ps.node.Info().Address]
		if !ok {
			message := subscribeMessage{topic}
			ps.broadcast(message)
		}
	}
	ps.postCommand(command)
}
func (ps *pubsub) GetSubscribers(topic string) []string {
	c := make(chan []string)
	ps.GetSubscribersAsync(topic, c)
	return <-c
}

func (ps *pubsub) GetSubscribersAsync(topic string, resultChan chan []string) {
	command := func() {
		subs, ok := ps.subscribers[topic]
		if ok {
			length := len(subs)
			if length == 0 {
				resultChan <- nil
			} else {
				results := make([]string, 0, length)
				for k, _ := range subs {
					results = append(results, k)
				}
				resultChan <- results
			}
		} else {
			resultChan <- nil
		}
	}
	ps.postCommand(command)
}

func (ps *pubsub) Publish(topic string, message interface{}) {
	command := func() {
		subs, ok := ps.subscribers[topic]
		if ok {
			pm := publishMessage{topic, message}
			for sub, _ := range subs {
				if sub == ps.node.Info().Address {
					ps.handleMessage(sub, pm)
				} else {
					ps.node.SendTo(sub, pm)
				}
			}
		}
	}
	ps.postCommand(command)
}

func (ps *pubsub) Unsubscribe(topic string) {
	command := func() {
		subs, ok := ps.subscribers[topic]
		if ok {
			_, ok := subs[ps.node.Info().Address]
			if ok {
				message := unsubscribeMessage{topic}
				ps.broadcast(message)
			}
		}
	}
	ps.postCommand(command)
}

func (ps *pubsub) getOrCreateSubscribers(topic string) map[string]bool {
	result, ok := ps.subscribers[topic]
	if !ok {
		result = make(map[string]bool)
		ps.subscribers[topic] = result
	}
	return result
}

func (ps *pubsub) getOrCreateNodeTopics(address string) map[string]bool {
	result, ok := ps.nodeTopics[address]
	if !ok {
		result = make(map[string]bool)
		ps.nodeTopics[address] = result
	}
	return result
}

func (ps *pubsub) addSubscriber(topic string, address string) {
	subs := ps.getOrCreateSubscribers(topic)
	subs[address] = true
	topics := ps.getOrCreateNodeTopics(address)
	topics[topic] = true
}

func (ps *pubsub) removeSubscriber(topic string, address string) {
	subs, ok := ps.subscribers[topic]
	if ok {
		delete(subs, address)
	}
	topics, ok := ps.nodeTopics[address]
	if ok {
		delete(topics, topic)
	}
}

func (ps *pubsub) handleMessage(address string, message interface{}) bool {
	switch message.(type) {
	case subscribedTopicsMessage:
		for _, topic := range message.(subscribedTopicsMessage).Topics {
			ps.addSubscriber(topic, address)
		}
		return true
	case subscribeMessage:
		ps.addSubscriber(message.(subscribeMessage).Topic, address)
		return true
	case unsubscribeMessage:
		ps.removeSubscriber(message.(unsubscribeMessage).Topic, address)
		return true
	case publishMessage:
		pm := message.(publishMessage)
		ps.onMessage(pm.Topic, pm.Message)
		return true
	default:
		return false
	}
	return false
}

func (ps *pubsub) Start() error {
	ps.Lock()
	defer ps.Unlock()
	if ps.isStarted {
		return errors.New("Already started.")
	}
	ps.isStarted = true
	go ps.loop()
	return nil
}

func (ps *pubsub) Stop() error {
	defer recover()
	ps.commandChan.Close()
	return nil
}

func (ps *pubsub) WaitForDone() {
}

func (ps *pubsub) loop() {
	for command := range ps.commandChan.In {
		command.(func())()
	}
}
