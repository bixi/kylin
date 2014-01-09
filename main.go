// kylin project main.go
package main

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"io"
	"kylin/cluster"
	"log"
	"net/http"
	"reflect"
)

// echo back the websocket.
func EchoServer(ws *websocket.Conn) {
	fmt.Println(ws.Request().RemoteAddr)
	fmt.Println(ws.RemoteAddr().String())
	io.Copy(ws, ws)
}

func main() {
	config := cluster.Config{"localhost:12345", []string{"localhost:12346"}}
	http.Handle("/echo", websocket.Handler(EchoServer))
	go func() {
		err := http.ListenAndServe(":12346", nil)
		if err != nil {
			panic(err.Error())
		}

	}()
	{
		ws, err := websocket.Dial("ws://localhost:12346/echo", "", config.Address)
		if err != nil {
			panic(err.Error())
		}
		if _, err := ws.Write([]byte("hello, world!\n")); err != nil {
			log.Fatal(err)
		}
		var msg = make([]byte, 512)
		if _, err := ws.Read(msg); err != nil {
			log.Fatal(err)
		}
		fmt.Println(msg)
	}

}
