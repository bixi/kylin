package cluster

import (
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/bixi/kylin/message"
	"log"
	"net"
	"net/http"
	"sync"
)

const (
	NotifyPath  = "/kylin/cluster/notify"
	ListPath    = "/kylin/cluster/list"
	ConnectPath = "/kylin/cluster/connect"
)

type NodeJoinListener func(*NodeConnection)

type Node struct {
	config            Config
	nodeJoinListeners []NodeJoinListener
	nodes             map[string]*NodeConnection
	server            *http.Server
	netListener       net.Listener
	isInitialized     bool
	mutex             sync.Mutex
}

var innerMessagesRegistered = false
var registerMutex = sync.Mutex{}

func NewNode(config Config) *Node {
	var node Node
	node.config = config
	node.nodes = make(map[string]*NodeConnection)
	mux := http.NewServeMux()
	mux.HandleFunc(NotifyPath, node.handleNotify)
	mux.HandleFunc(ListPath, node.handleList)
	mux.Handle(ConnectPath, websocket.Handler(node.handleConnect))

	node.server = &http.Server{Addr: config.Address, Handler: mux}
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

func registerInnerMessages() {
	registerMutex.Lock()
	defer registerMutex.Unlock()
	if innerMessagesRegistered {
		return
	}
	innerMessagesRegistered = true
	gob.Register(NodeInfo{})
}

func (node *Node) Start() error {
	registerInnerMessages()
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.isInitialized {
		return errors.New("Node has started.")
	}
	go func() {
		var err error
		node.netListener, err = net.Listen("tcp", node.server.Addr)
		if err != nil {
			log.Printf("Node.Start() error:%v", err)
		}
		err = node.server.Serve(node.netListener)
		if err != nil {
			log.Printf("Node.Start() error:%v", err)
		}
	}()
	return nil
}

func (node *Node) Stop() error {
	if node.netListener != nil {
		return node.netListener.Close()
	} else {
		return nil
	}
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
		log.Printf("Node notify error:%s", err.Error())
		return
	}
	if determineHostAddress(node.Info().Address, info.Address) != info.Address {
		log.Printf("Invalid node notification: local %s, remote %s",
			node.Info().Address, info.Address)
		return
	}
	if node.isConnected(info.Address) {
		return
	}
	nodeConnection, err := node.connect(info)
	if err == nil {
		node.addNode(nodeConnection)
		nodeConnection.Send(node.Info())
		<-nodeConnection.transporter.Done
	} else {
		log.Printf("node connect error:%v,%v", nodeConnection.Info().Address, err)
	}
}

func (node *Node) handleConnect(conn *websocket.Conn) {
	decoder := gob.NewDecoder(conn)
	var info interface{}
	decoder.Decode(&info)
	nodeConnection, err := node.setupConnection(conn, info.(NodeInfo))
	if err != nil {
		node.addNode(nodeConnection)
		<-nodeConnection.transporter.Done
	} else {
		log.Printf("node connect error:%v,%v", nodeConnection.Info().Address, err)

	}
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
	node           *Node
	nodeConnection *NodeConnection
	gobEncoder     *gob.Encoder
	gobDecoder     *gob.Decoder
}

func newNodeHandler(node *Node, nodeConnection *NodeConnection) *nodeHandler {
	var nodeHandler nodeHandler
	nodeHandler.node = node
	nodeHandler.nodeConnection = nodeConnection
	nodeHandler.gobEncoder = gob.NewEncoder(nodeConnection.conn)
	nodeHandler.gobDecoder = gob.NewDecoder(nodeConnection.conn)
	return &nodeHandler
}

func (nodeHandler *nodeHandler) Encode(message interface{}) error {
	return nodeHandler.gobEncoder.Encode(&message)
}

func (nodeHandler *nodeHandler) Decode(message interface{}) error {
	return nodeHandler.gobDecoder.Decode(message)
}

func (nodeHandler *nodeHandler) Handle(err error) {
	log.Printf("Node %v error:%v", nodeHandler.nodeConnection.conn.RemoteAddr(), err)
}

func (nodeHandler *nodeHandler) Dispatch(message interface{}) error {
	return nil
}

func (node *Node) onNodeJoin(nodeConnection *NodeConnection) {
	for i := 0; i < len(node.nodeJoinListeners); i++ {
		listener := node.nodeJoinListeners[i]
		listener(nodeConnection)
	}
}

func (node *Node) isConnected(address string) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	_, ok := node.nodes[info.Address]
	return ok
}

func (node *Node) addNode(nodeConnection *NodeConnection) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.nodes[nodeConnection.Info().Address] = nodeConnection
	node.onNodeJoin(nodeConnection)
}

func (node *Node) setupConnection(conn *websocket.Conn, info NodeInfo) (*NodeConnection, error) {
	nodeConnection := &NodeConnection{}
	nodeConnection.info = info
	nodeConnection.conn = conn
	handler := newNodeHandler(node, nodeConnection)
	nodeConnection.transporter = message.NewTransporter(handler, handler, handler, handler)
	nodeConnection.transporter.Start()
	return nodeConnection, nil
}

func (node *Node) connect(info NodeInfo) (*NodeConnection, error) {
	address := "ws://" + info.Address + ConnectPath
	origin := "ws://" + node.Info().Address
	conn, err := websocket.Dial(address, "", origin)
	if err != nil {
		return nil, err
	}
	return node.setupConnection(conn, info)
}
