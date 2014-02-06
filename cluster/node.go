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
	WaitForDone()
	AddNodeJoinListener(NodeJoinListener)
	AddNodeDropListener(NodeDropListener)
	List() []NodeInfo
	RegisterMessage(message interface{})
}

type node struct {
	sync.Mutex
	config            Config
	nodeJoinListeners []NodeJoinListener
	nodeDropListeners []NodeDropListener
	nodes             map[string]NodeConnection
	connectingNodes   map[string]bool
	commandChan       chan func()
	server            *http.Server
	netListener       net.Listener
	isInitialized     bool
	stopChan          chan struct{}
	done              chan struct{}
}

type nodeFoundMessage struct {
	Address string
}

func NewNode(config Config) Node {
	var n node
	n.config = config
	n.nodes = make(map[string]NodeConnection)
	n.connectingNodes = make(map[string]bool)
	n.nodeJoinListeners = make([]NodeJoinListener, 0, 1)
	n.nodeDropListeners = make([]NodeDropListener, 0, 1)
	n.commandChan = make(chan func(), 1000)
	n.stopChan = make(chan struct{}, 1)
	n.done = make(chan struct{}, 1)
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
	n.Lock()
	defer n.Unlock()
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
	n.Lock()
	defer n.Unlock()
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
	defer recover()
	gob.Register(message)
}

func (n *node) WaitForDone() {
	defer recover()
	<-n.done
}

func registerInnerMessages() {
	defer recover()
	gob.Register(nodeFoundMessage{})
}

func (n *node) Start() error {
	registerInnerMessages()
	n.Lock()
	defer n.Unlock()
	if n.isInitialized {
		return errors.New("node has started.")
	}
	n.isInitialized = true
	go n.loop()
	return nil
}

func (n *node) Stop() error {
	n.Lock()
	defer n.Unlock()
	if !n.isInitialized {
		return errors.New("node not started.")
	}
	defer recover()
	close(n.stopChan)
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
	nodeConnection := n.createNodeConnection(conn, info)
	command := func() {
		n.processNodeConnected(nodeConnection, resultChan)
	}
	n.commandChan <- command
	nodeConnection = <-resultChan
	if nodeConnection != nil {
		n.waitForClose(nodeConnection)
	}
}

func (n *node) createNodeConnection(conn *websocket.Conn, info NodeInfo) NodeConnection {
	nodeConnection := newNodeConnection(n, conn, info)
	nodeConnection.AddMessageHandler(n.handleConnectionMessage)
	return nodeConnection
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
	for _, connection := range n.nodes {
		if connection != nodeConnection {
			connection.Send(nodeFoundMessage{nodeConnection.Info().Address})
			nodeConnection.Send(nodeFoundMessage{connection.Info().Address})
		}
	}
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
				log.Printf("node connect error:%v,%v \n", address, err)
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
		nc, e := n.connect(address, n.Info())
		command := func() {
			callback(nc, e)
		}
		n.commandChan <- command
	}()
}

func (n *node) connect(address string, localInfo NodeInfo) (NodeConnection, error) {
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
	return n.createNodeConnection(conn, remoteInfo), nil
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

func (n *node) listen() {
	var err error
	n.netListener, err = net.Listen("tcp", n.config.Address)
	if err != nil {
		log.Printf("net.Listen error:%v \n", err)
	}
}

func (n *node) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc(NotifyPath, n.handleNotify)
	mux.HandleFunc(ListPath, n.handleList)
	mux.Handle(ConnectPath, websocket.Handler(n.handleConnect))
	n.server = &http.Server{Addr: n.config.Address, Handler: mux}
	err := n.server.Serve(n.netListener)
	if err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("server.Serve error:%v \n", err)
		}
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

func (n *node) handleConnectionMessage(message interface{}) bool {
	switch message.(type) {
	case nodeFoundMessage:
		command := func() {
			address := message.(nodeFoundMessage).Address
			n.detectNode(address)
		}
		n.commandChan <- command
		return true
	default:
		return false
	}
}

func (n *node) loop() {
	n.listen()
	go n.serve()
	n.detectNodes(n.config.Nodes)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	isStopped := false
	for {
		if isStopped {
			select {
			case command := <-n.commandChan:
				command()
			default:
				if len(n.nodes) > 0 {
					for _, nc := range n.nodes {
						nc.Stop()
					}
				} else {
					if len(n.connectingNodes) == 0 {
						defer recover()
						close(n.done)
						return
					}
				}
			}
		} else {
			select {
			case command := <-n.commandChan:
				command()
			case <-ticker.C:
				n.detectNodes(n.config.Nodes)
			case <-n.stopChan:
				for _, nc := range n.nodes {
					nc.Stop()
				}
				if n.netListener != nil {
					err := n.netListener.Close()
					if err != nil {
						log.Printf("Close net listener error:%v \n", err)
					}
				}
				isStopped = true
			}
		}
	}
}
