package cluster

import (
	"bytes"
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	NotifyPath  = "/kylin/cluster/notify"
	ListPath    = "/kylin/cluster/list"
	ConnectPath = "/kylin/cluster/connect"
)

type NodeJoinListener func(NodeConnection)
type NodeDropListener func(NodeConnection)

type Node interface {
	Info() NodeInfo
	Start() error
	Stop() error
	AddNodeJoinListener(NodeJoinListener)
	AddNodeDropListener(NodeDropListener)
	List() []NodeInfo
	RegisterMessage(message interface{})
}

type node struct {
	config            Config
	nodeJoinListeners []NodeJoinListener
	nodeDropListeners []NodeDropListener
	nodes             map[string]NodeConnection
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

func NewNode(config Config) Node {
	var n node
	n.config = config
	n.nodes = make(map[string]NodeConnection)
	n.connectingNodes = make(map[string]bool)
	n.nodeJoinListeners = make([]NodeJoinListener, 0, 1)
	n.nodeDropListeners = make([]NodeDropListener, 0, 1)
	n.commandChan = make(chan func(), 1000)
	n.stopChan = make(chan bool, 1)
	return &n
}

func (n *node) Info() NodeInfo {
	return NodeInfo{n.config.Address, n.config.Description}
}

func (n *node) List() []NodeInfo {
	resultChan := make(chan []NodeInfo)
	command := func() {
		list := make([]NodeInfo, 0, len(n.nodes)+1)
		list = append(list, n.Info())
		for _, value := range n.nodes {
			list = append(list, value.Info())
		}
		resultChan <- list
	}
	n.commandChan <- command
	list := <-resultChan
	return list
}

func (n *node) AddNodeJoinListener(listener NodeJoinListener) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.isInitialized {
		command := func() {
			n.nodeJoinListeners = append(n.nodeJoinListeners, listener)
		}
		n.commandChan <- command
	} else {
		n.nodeJoinListeners = append(n.nodeJoinListeners, listener)
	}
}

func (n *node) AddNodeDropListener(listener NodeDropListener) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.isInitialized {
		command := func() {
			n.nodeDropListeners = append(n.nodeDropListeners, listener)
		}
		n.commandChan <- command
	} else {
		n.nodeDropListeners = append(n.nodeDropListeners, listener)
	}
}

func (n *node) RegisterMessage(message interface{}) {
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

func (n *node) Start() error {
	registerInnerMessages()
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.isInitialized {
		return errors.New("node has started.")
	}
	n.isInitialized = true
	go n.loop()
	return nil
}

func (n *node) Stop() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if !n.isInitialized {
		return errors.New("node not started.")
	}
	n.stopChan <- true
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

func (n *node) handleList(w http.ResponseWriter, r *http.Request) {
	list := n.List()
	encoder := json.NewEncoder(w)
	err := encoder.Encode(list)
	if err != nil {
		log.Printf("node list error:%s \n", err.Error())
	}
}

func (n *node) handleNotify(w http.ResponseWriter, r *http.Request) {
	info, err := readNodeInfo(r.Body)
	if err != nil {
		log.Printf("node notify error:%s \n", err.Error())
		return
	}
	if determineHostAddress(n.Info().Address, info.Address) != info.Address {
		log.Printf("Invalid n notification: local %s, remote %s \n",
			n.Info().Address, info.Address)
		return
	}
	n.connectRoutine(info.Address)
}

func (n *node) handleConnect(conn *websocket.Conn) {
	info, err := readNodeInfo(conn)
	if err != nil {
		log.Printf("handle connect error:%v,%v \n", conn.RemoteAddr(), err)
		return
	}
	err = writeNodeInfo(n.Info(), conn)
	if err != nil {
		log.Printf("handle connect error:%v,%v \n", conn.RemoteAddr(), err)
		return
	}
	resultChan := make(chan NodeConnection, 1)
	nodeConnection := newNodeConnection(conn, info)
	command := func() {
		n.processNodeConnected(nodeConnection, resultChan)
	}
	n.commandChan <- command
	nodeConnection = <-resultChan
	if nodeConnection != nil {
		n.waitForClose(nodeConnection)
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
	n              *node
	nodeConnection NodeConnection
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

func newNodeHandler(nodeConnection NodeConnection) *nodeHandler {
	var nodeHandler nodeHandler
	nodeHandler.nodeConnection = nodeConnection
	nodeHandler.gobEncoder = gob.NewEncoder(nodeConnection.Conn())
	nodeHandler.gobDecoder = gob.NewDecoder(nodeConnection.Conn())
	return &nodeHandler
}

func (nodeHandler *nodeHandler) Encode(message interface{}) error {
	return nodeHandler.gobEncoder.Encode(&message)
}

func (nodeHandler *nodeHandler) Decode(message interface{}) error {
	return nodeHandler.gobDecoder.Decode(message)
}

func (nodeHandler *nodeHandler) Handle(err error) {
	log.Printf("node %v error:%v \n", nodeHandler.nodeConnection.Conn().RemoteAddr(), err)
}

func (nodeHandler *nodeHandler) Dispatch(message interface{}) error {
	return nil
}

func (n *node) onNodeJoin(nodeConnection NodeConnection) {
	for i := 0; i < len(n.nodeJoinListeners); i++ {
		listener := n.nodeJoinListeners[i]
		listener(nodeConnection)
	}
}

func (n *node) onNodeDrop(nodeConnection NodeConnection) {
	for i := 0; i < len(n.nodeDropListeners); i++ {
		listener := n.nodeDropListeners[i]
		listener(nodeConnection)
	}
}

func (n *node) isConnectedOrConnecting(address string) bool {
	_, connected := n.nodes[address]
	_, connecting := n.connectingNodes[address]
	isMyself := address == n.Info().Address
	return connected || connecting || isMyself
}

func (n *node) addNode(nodeConnection NodeConnection) {
	n.nodes[nodeConnection.Info().Address] = nodeConnection
}

func (n *node) removeNode(nodeConnection NodeConnection) {
	delete(n.nodes, nodeConnection.Info().Address)
	n.onNodeDrop(nodeConnection)
}

func (n *node) addConnecting(address string) {
	n.connectingNodes[address] = true
}

func (n *node) processNodeConnected(nodeConnection NodeConnection, resultChan chan NodeConnection) {
	err := nodeConnection.Start()
	if err != nil {
		log.Printf("n connect error:%v,%v \n", nodeConnection.Info().Address, err)
		resultChan <- nil
		return
	}
	n.addNode(nodeConnection)
	n.onNodeJoin(nodeConnection)
	resultChan <- nodeConnection
}

func (n *node) removeConnecting(address string) {
	delete(n.connectingNodes, address)
}

type connectResult struct {
	nodeConnection NodeConnection
	err            error
}

func (n *node) connectRoutine(address string) {
	resultChan := make(chan NodeConnection, 1)
	command := func() {
		if n.isConnectedOrConnecting(address) {
			resultChan <- nil
			return
		}
		n.addConnecting(address)
		n.connectAsync(address, func(nodeConnection NodeConnection, err error) {
			n.removeConnecting(address)
			if err != nil {
				log.Printf("n connect error:%v,%v \n", address, err)
				resultChan <- nil
				return
			}
			n.processNodeConnected(nodeConnection, resultChan)
		})
	}
	n.commandChan <- command
	nodeConnection := <-resultChan
	if nodeConnection != nil {
		n.waitForClose(nodeConnection)
	}
}

func (n *node) waitForClose(nodeConnection NodeConnection) {
	nodeConnection.WaitForDone()
	command := func() {
		n.removeNode(nodeConnection)
	}
	n.commandChan <- command
}

func (n *node) connectAsync(address string, callback func(NodeConnection, error)) {
	go func() {
		nc, e := connect(address, n.Info())
		command := func() {
			callback(nc, e)
		}
		n.commandChan <- command
	}()
}

func connect(address string, localInfo NodeInfo) (NodeConnection, error) {
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
	return newNodeConnection(conn, remoteInfo), nil
}

func (n *node) notify(address string) {
	go func() {
		url := getNotifyUrl(address)
		buffer := &bytes.Buffer{}
		err := writeNodeInfo(n.Info(), buffer)
		if err == nil {
			http.Post(url, "application/json", strings.NewReader(string(buffer.Bytes())))
		} else {
			log.Printf("Write n info error:%v \n", err)
		}
	}()
}

func (n *node) listenAndServe() {
	mux := http.NewServeMux()
	mux.HandleFunc(NotifyPath, n.handleNotify)
	mux.HandleFunc(ListPath, n.handleList)
	mux.Handle(ConnectPath, websocket.Handler(n.handleConnect))
	n.server = &http.Server{Addr: n.config.Address, Handler: mux}
	var err error
	n.netListener, err = net.Listen("tcp", n.server.Addr)
	if err != nil {
		log.Printf("net.Listen error:%v \n", err)
		return
	}
	err = n.server.Serve(n.netListener)
	if err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("server.Serve error:%v \n", err)
		}
		return
	}
}

func (n *node) detectNodes(remoteAddresses []string) {
	for _, remoteAddress := range remoteAddresses {
		n.detectNode(remoteAddress)
	}
}

func (n *node) detectNode(remoteAddress string) {
	if n.isConnectedOrConnecting(remoteAddress) {
		return
	}
	myAddress := n.Info().Address
	hostAddress := determineHostAddress(myAddress, remoteAddress)
	if hostAddress == myAddress {
		n.notify(remoteAddress)
	} else if hostAddress == remoteAddress {
		go n.connectRoutine(hostAddress)
	}
}

func (n *node) loop() {
	go n.listenAndServe()
	n.detectNodes(n.config.Nodes)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case command := <-n.commandChan:
			command()
		case <-ticker.C:
			n.detectNodes(n.config.Nodes)
		case <-n.stopChan:
			if n.netListener != nil {
				err := n.netListener.Close()
				if err != nil {
					log.Printf("Close net listener error:%v \n", err)
				}
			}
			break
		}
	}
}
