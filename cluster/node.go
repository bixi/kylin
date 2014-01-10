package cluster

import (
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/bixi/kylin/message"
	"log"
	"net/http"
	"reflect"
	"sync"
)

const (
	NotifyPath  = "/kylin/cluster/notify"
	ListPath    = "/kylin/cluster/list"
	ConnectPath = "/kylin/cluster/connect"
)

type NodeJoinListener func(NodeInfo)
type MessageHandler func(interface{}) bool

type nodeData struct {
	info        NodeInfo
	transporter *message.Transporter
	conn        *websocket.Conn
}

type Node struct {
	config            Config
	nodeJoinListeners []NodeJoinListener
	messageHandlers   []MessageHandler
	nodes             map[string]*nodeData
	isInitialized     bool
	mutex             sync.Mutex
}

func NewNode(config Config) *Node {
	var node Node
	node.config = config
	node.nodes = make(map[string]*nodeData)
	return &node
}

func (node *Node) Info() NodeInfo {
	return NodeInfo{node.config.Address, node.config.Description}
}

func (node *Node) List() []NodeInfo {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	list := make([]NodeInfo, 0, len(node.nodes)+1)
	list = append(list, node.Info())
	for _, value := range node.nodes {
		list = append(list, value.info)
	}
	return list
}

func (node *Node) AddNodeJoinListener(listener NodeJoinListener) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.nodeJoinListeners = append(node.nodeJoinListeners, listener)
}

func (node *Node) RegisterMessage(message interface{}) {
	gob.Register(message)
}

func (node *Node) Start() error {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.isInitialized {
		return errors.New("Node has started.")
	}
	mux := http.NewServeMux()
	mux.HandleFunc(NotifyPath, node.handleNotify)

	return nil
}

func (node *Node) Stop() error {
	return nil
}

func (node *Node) handleList(w http.ResponseWriter, r *http.Request) {
	list := node.List()
	encoder := json.NewEncoder(w)
	err := encoder.Encode(list)
	if err != nil {
		log.Panic("Node list error:%s", err.Error())
	}
}

func (node *Node) handleNotify(w http.ResponseWriter, r *http.Request) {
	info := NodeInfo{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&info)
	if err != nil {
		log.Panic("Node notify error:%s", err.Error())
		return
	}
	if determineHostAddress(node.Info().Address, info.Address) != info.Address {
		log.Panic("Invalid node notification: local %s, remote %s",
			node.Info().Address, info.Address)
		return
	}

	// TODO connect
}

func (node *Node) handleConnect(ws *websocket.Conn) {

}

func determineHostAddress(addr1 string, addr2 string) string {
	if addr1 < addr2 {
		return addr1
	} else if addr2 < addr1 {
		return addr2
	} else {
		return "" // self, do not need to connect.
	}
}

type nodeHandler struct {
	node       *Node
	gobEncoder *gob.Encoder
	gobDecoder *gob.Decoder
}

func newNodeHandler(node *Node, conn *websocket.Conn) *nodeHandler {
	nodeHandler := nodeHandler{}
	nodeHandler.node = node
	nodeHandler.gobEncoder = gob.NewEncoder(conn)
	nodeHandler.gobDecoder = gob.NewDecoder(conn)
	return &nodeHandler
}

func (nodeHandler *nodeHandler) Encode(message interface{}) error {
	if reflect.TypeOf(message).Kind() == reflect.Ptr {
		return nodeHandler.gobEncoder.Encode(message)
	} else {
		return nodeHandler.gobEncoder.Encode(&message)
	}
}

func (nodeHandler *nodeHandler) Decode() (message interface{}, err error) {
	err = nodeHandler.gobDecoder.Decode(&message)
	return message, err
}

func (node *Node) connect(info NodeInfo) error {
	address := "ws://" + info.Address + ConnectPath
	origin := "ws://" + node.Info().Address
	conn, err := websocket.Dial(address, "", origin)
	if err != nil {
		return err
	}
	node.mutex.Lock()
	defer node.mutex.Unlock()
	data := nodeData{}
	data.info = info
	data.conn = conn
	return nil
}
