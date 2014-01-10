package message

import (
	"code.google.com/p/go.net/websocket"
	"encoding/gob"
	"log"
	"net/http"
	"reflect"
	"testing"
)

// echo back the websocket.
func testTransporterEchoServer(ws *websocket.Conn) {
	var transporter *Transporter
	onMessage := DispatcherFunc(func(message interface{}) error {
		transporter.Send(message)
		transporter.Stop()
		return nil
	})
	onError := ErrorHandlerFunc(func(err error) {
		log.Println(err)
	})
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
	return EncoderFunc(encoder)
}

func getDecoder(ws *websocket.Conn) Decoder {
	gobDecoder := gob.NewDecoder(ws)
	decoder := func() (interface{}, error) {
		var buf interface{}
		err := gobDecoder.Decode(&buf)
		return buf, err
	}
	return DecoderFunc(decoder)
}

func TestTransporter(t *testing.T) {
	gob.Register(testMessage{})
	http.Handle("/echo", websocket.Handler(testTransporterEchoServer))
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

	onMessage := DispatcherFunc(func(message interface{}) error {
		result = message.(testMessage).Info
		transporter.Stop()
		return nil
	})
	onError := ErrorHandlerFunc(func(err error) {
		log.Println(err)
	})
	transporter = NewTransporter(getEncoder(ws), getDecoder(ws), onMessage, onError)
	transporter.Start()
	transporter.Send(&testMessage{"Hello"})
	<-transporter.Done
	if result != "Hello" {
		t.Fail()
	}
}
