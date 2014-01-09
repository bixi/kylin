package message

import (
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"log"
	"net/http"
	"testing"
)

// echo back the websocket.
func EchoServer(ws *websocket.Conn) {
	var transporter *Transporter
	onMessage := func(message interface{}) {
		transporter.Send(message)
		transporter.Stop()
	}
	onError := func(err error) {
		log.Println(err)
	}
	transporter = NewTransporter(getEncoder(ws), getDecoder(ws), onMessage, onError)
	transporter.Start()
	<-transporter.Done
}

type testMessage struct {
	Info string
}

func getEncoder(ws *websocket.Conn) Encoder {
	gobEncoder := gob.NewEncoder(ws)
	encoder := func(message interface{}) error {
		return gobEncoder.Encode(&message)
	}
	return encoder
}

func getDecoder(ws *websocket.Conn) Decoder {
	gobDecoder := gob.NewDecoder(ws)
	decoder := func() (interface{}, error) {
		var buf interface{}
		err := gobDecoder.Decode(&buf)
		return buf, err
	}
	return decoder
}

func TestConn(t *testing.T) {
	gob.Register(testMessage{})
	http.Handle("/echo", websocket.Handler(EchoServer))
	go func() {
		err := http.ListenAndServe(":12344", nil)
		if err != nil {
			t.Fatal(err.Error())
		}
	}()

	ws, err := websocket.Dial("ws://localhost:12344/echo", "", "ws://localhost/client")
	if err != nil {
		t.Fatal(err.Error())
	}

	var transporter *Transporter
	var result string

	onMessage := func(message interface{}) {
		result = message.(testMessage).Info
		transporter.Stop()
	}
	onError := func(err error) {
		log.Println(err)
	}
	transporter = NewTransporter(getEncoder(ws), getDecoder(ws), onMessage, onError)
	transporter.Start()
	transporter.Send(testMessage{"Hello"})
	<-transporter.Done
	if result != "Hello" {
		t.Fail()
	}
}
