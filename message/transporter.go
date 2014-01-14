package message

import (
	"errors"
	"io"
	"strings"
	"sync"
)

type Transporter interface {
	Start() error
	Stop() error
	Send(message interface{})
	WaitForDone()
}

type transporter struct {
	done         <-chan bool
	mutex        sync.Mutex
	encoder      Encoder
	decoder      Decoder
	dispatcher   Dispatcher
	errorHandler ErrorHandler
	sendbox      chan []interface{}
	isRunning    bool
	isStopped    bool
}

func NewTransporter(encoder Encoder,
	decoder Decoder,
	dispatcher Dispatcher,
	errorHandler ErrorHandler) Transporter {
	var t transporter
	t.encoder = encoder
	t.decoder = decoder
	t.dispatcher = dispatcher
	t.errorHandler = errorHandler
	t.sendbox = make(chan []interface{}, 1)
	return &t
}

func (t *transporter) Start() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.isRunning {
		return errors.New("transporter is running.")
	}

	t.isRunning = true
	done := make(chan bool, 1)
	t.done = done

	//sending goroutine
	go func() {
		for messages := range t.sendbox {
			err := t.processSendingMessages(messages)
			if err != nil {
				t.handleError(err)
				break
			}
		}
		done <- true
	}()

	//receving goroutine
	go func() {
		for {
			var message interface{}
			err := t.decoder.Decode(&message)
			if err == nil {
				t.dispatcher.Dispatch(message)
			} else {
				t.handleError(err)
				t.Stop()
				return
			}
		}
	}()

	return nil
}

func (t *transporter) Send(message interface{}) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	select {
	case messages := <-t.sendbox:
		messages = append(messages, message)
		t.sendbox <- messages
	default:
		messages := []interface{}{message}
		t.sendbox <- messages
	}
}

func (t *transporter) Stop() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.isStopped {
		return errors.New("transporter has stopped.")
	}
	t.isStopped = true
	close(t.sendbox)
	return nil
}

func (t *transporter) WaitForDone() {
	<-t.done
}

func shouldReport(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return false
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return false
	}
	return true
}

func (t *transporter) processSendingMessages(messages []interface{}) error {
	for _, message := range messages {
		err := t.encoder.Encode(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *transporter) handleError(err error) {
	if t.errorHandler != nil {
		if shouldReport(err) {
			t.errorHandler.Handle(err)
		}
	}
}
