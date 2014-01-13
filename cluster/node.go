package cluster

import (
	"bytes"
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/bixi/kylin/message"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
)

const (
	NotifyPath  = "/kylin/cluster/notify"
	ListPath    = "/kylin/cluster/list"
	ConnectPath = "/kylin/cluster/connect"
)

type NodeJoinListener func(*NodeConnection)
type NodeDropListener func(*NodeConnection)

type Node struct {
	config            Config
	nodeJoinListeners []NodeJoinListener
	nodeDropListeners []NodeDropListener
	nodes             map[string]*NodeConnection
	connectingNodes   map[string]bool
	commandChan       chan func()
	server            *http.Server
	netListener       net.Listener
	isInitialized     bool
	stopChan          chan bool
	mutex             sync.Mutex
}

var innerMessagesRegistered = false
var registerMutex = sync.Mutex{}

func NewNode(config Config) *Node {
	var node Node
	node.config = config
	node.nodes = make(map[string]*NodeConnection)
	node.connectingNodes = make(map[string]bool)
	node.nodeJoinListeners = make([]NodeJoinListener, 0, 1)
	node.nodeDropListeners = make([]NodeDropListener, 0, 1)
	node.commandChan = make(chan func(), 1000)
	node.stopChan = make(chan bool, 1)
	return &node
}

func (node *Node) Info() NodeInfo {
	return NodeInfo{node.config.Address, node.config.Description}
}

func (node *Node) List() []NodeInfo {
	resultChan := make(chan []NodeInfo)
	command := func() {
		list := make([]NodeInfo, 0, len(node.nodes)+1)
		list = append(list, node.Info())
		for _, value := range node.nodes {
			list = append(list, value.info)
		}
		resultChan <- list
	}
	node.commandChan <- command
	list := <-resultChan
	return list
}

func (node *Node) AddNodeJoinListener(listener NodeJoinListener) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.isInitialized {
		command := func() {
			node.nodeJoinListeners = append(node.nodeJoinListeners, listener)
		}
		node.commandChan <- command
	} else {
		node.nodeJoinListeners = append(node.nodeJoinListeners, listener)
	}
}

func (node *Node) AddNodeDropListener(listener NodeDropListener) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.isInitialized {
		command := func() {
			node.nodeDropListeners = append(node.nodeDropListeners, listener)
		}
		node.commandChan <- command
	} else {
		node.nodeDropListeners = append(node.nodeDropListeners, listener)
	}
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
}

func (node *Node) Start() error {
	registerInnerMessages()
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.isInitialized {
		return errors.New("Node has started.")
	}
	node.isInitialized = true
	go node.loop()
	return nil
}

func (node *Node) Stop() error {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if !node.isInitialized {
		return errors.New("Node not started.")
	}
	node.stopChan <- true
	return nil

}

func getNotifyUrl(address string) string {
	return "http://" + address + NotifyPath
}

func getListUrl(address string) string {
	return "http://" + address + ListPath
}

func getConnectUrl(address string) string {
	return "ws://" + address + ConnectPath
}

func (node *Node) handleList(w http.ResponseWriter, r *http.Request) {
	list := node.List()
	encoder := json.NewEncoder(w)
	err := encoder.Encode(list)
	if err != nil {
		log.Printf("Node list error:%s \n", err.Error())
	}
}

func (node *Node) handleNotify(w http.ResponseWriter, r *http.Request) {
	info, err := readNodeInfo(r.Body)
	if err != nil {
		log.Printf("Node notify error:%s \n", err.Error())
		return
	}
	if determineHostAddress(node.Info().Address, info.Address) != info.Address {
		log.Printf("Invalid node notification: local %s, remote %s \n",
			node.Info().Address, info.Address)
		return
	}
	node.connectRoutine(info.Address)
}

func (node *Node) handleConnect(conn *websocket.Conn) {
	info, err := readNodeInfo(conn)
	if err == nil {
		resultChan := make(chan *NodeConnection)
		command := func() {
			nodeConnection, err := setupConnection(conn, info)
			if err == nil {
				node.addNode(nodeConnection)
			} else {
				log.Printf("node connect error:%v,%v \n", conn.RemoteAddr(), err)
			}
			resultChan <- nodeConnection
		}
		node.commandChan <- command
		nodeConnection := <-resultChan
		if nodeConnection != nil {
			writeNodeInfo(node.Info(), conn)
			nodeConnection.Start()
			<-nodeConnection.transporter.Done
			command = func() {
				node.removeNode(nodeConnection)
			}
			node.commandChan <- command
		}
	} else {
		log.Printf("node connect error:%v,%v \n", conn.RemoteAddr(), err)
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

func readNodeInfo(r io.Reader) (NodeInfo, error) {
	decoder := json.NewDecoder(r)
	var info NodeInfo
	err := decoder.Decode(&info)
	return info, err
}

func writeNodeInfo(info NodeInfo, w io.Writer) error {
	encoder := json.NewEncoder(w)
	err := encoder.Encode(info)
	return err
}

func newNodeHandler(nodeConnection *NodeConnection) *nodeHandler {
	var nodeHandler nodeHandler
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
	log.Printf("Node %v error:%v \n", nodeHandler.nodeConnection.conn.RemoteAddr(), err)
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

func (node *Node) onNodeDrop(nodeConnection *NodeConnection) {
	for i := 0; i < len(node.nodeDropListeners); i++ {
		listener := node.nodeDropListeners[i]
		listener(nodeConnection)
	}
}

func (node *Node) isConnectedOrConnecting(address string) bool {
	_, connected := node.nodes[address]
	_, connecting := node.connectingNodes[address]
	isMyself := address == node.Info().Address
	return connected || connecting || isMyself
}

func (node *Node) addNode(nodeConnection *NodeConnection) {
	node.nodes[nodeConnection.Info().Address] = nodeConnection
	node.onNodeJoin(nodeConnection)
}

func (node *Node) removeNode(nodeConnection *NodeConnection) {
	delete(node.nodes, nodeConnection.Info().Address)
	node.onNodeDrop(nodeConnection)
}

func setupConnection(conn *websocket.Conn, info NodeInfo) (*NodeConnection, error) {
	nodeConnection := &NodeConnection{}
	nodeConnection.info = info
	nodeConnection.conn = conn
	handler := newNodeHandler(nodeConnection)
	nodeConnection.transporter = message.NewTransporter(handler, handler, handler, handler)
	return nodeConnection, nil
}

func (node *Node) addConnecting(address string) {
	node.connectingNodes[address] = true
}

func (node *Node) removeConnecting(address string) {
	delete(node.connectingNodes, address)
}

type connectResult struct {
	nodeConnection *NodeConnection
	err            error
}

func (node *Node) connectRoutine(address string) {
	resultChan := make(chan *NodeConnection)
	command := func() {
		if node.isConnectedOrConnecting(address) {
			resultChan <- nil
			return
		}
		node.addConnecting(address)
		node.connectAsync(address, func(nodeConnection *NodeConnection, err error) {
			node.removeConnecting(address)
			if err == nil {
				node.addNode(nodeConnection)
			} else {
				log.Printf("node connect error:%v,%v \n", address, err)
			}
			resultChan <- nodeConnection
		})
	}
	node.commandChan <- command
	nodeConnection := <-resultChan
	if nodeConnection != nil {
		nodeConnection.Start()
		<-nodeConnection.transporter.Done
		command = func() {
			node.removeNode(nodeConnection)
		}
		node.commandChan <- command
	}
}

func (node *Node) connectAsync(address string, callback func(*NodeConnection, error)) {
	go func() {
		n, e := connect(address, node.Info())
		command := func() {
			callback(n, e)
		}
		node.commandChan <- command
	}()
}

func connect(address string, localInfo NodeInfo) (*NodeConnection, error) {
	url := getConnectUrl(address)
	origin := "ws://" + localInfo.Address
	conn, err := websocket.Dial(url, "", origin)
	if err != nil {
		return nil, err
	}
	err = writeNodeInfo(localInfo, conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	remoteInfo, err := readNodeInfo(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return setupConnection(conn, remoteInfo)
}

func (node *Node) notify(address string) {
	go func() {
		url := getNotifyUrl(address)
		buffer := &bytes.Buffer{}
		err := writeNodeInfo(node.Info(), buffer)
		if err == nil {
			http.Post(url, "application/json", strings.NewReader(string(buffer.Bytes())))
		} else {
			log.Printf("Write node info error:%v \n", err)
		}
	}()
}

func (node *Node) listenAndServe() {
	mux := http.NewServeMux()
	mux.HandleFunc(NotifyPath, node.handleNotify)
	mux.HandleFunc(ListPath, node.handleList)
	mux.Handle(ConnectPath, websocket.Handler(node.handleConnect))
	node.server = &http.Server{Addr: node.config.Address, Handler: mux}
	var err error
	node.netListener, err = net.Listen("tcp", node.server.Addr)
	if err != nil {
		log.Printf("net.Listen error:%v \n", err)
		return
	}
	err = node.server.Serve(node.netListener)
	if err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("server.Serve error:%v \n", err)
		}
		return
	}
}

func (node *Node) detectNode(remoteAddress string) {
	if node.isConnectedOrConnecting(remoteAddress) {
		return
	}
	myAddress := node.Info().Address
	hostAddress := determineHostAddress(myAddress, remoteAddress)
	if hostAddress == myAddress {
		node.notify(remoteAddress)
	} else if hostAddress == remoteAddress {
		go node.connectRoutine(hostAddress)
	}
}

func (node *Node) loop() {
	go node.listenAndServe()
	for _, remoteAddress := range node.config.Nodes {
		node.detectNode(remoteAddress)
	}
	for {
		select {
		case command := <-node.commandChan:
			command()
		case <-node.stopChan:
			if node.netListener != nil {
				err := node.netListener.Close()
				if err != nil {
					log.Printf("Close net listener error:%v \n", err)
				}
			}
			break
		}
	}
}
