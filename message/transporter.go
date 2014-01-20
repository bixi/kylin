package message

import (
	"errors"
	"github.com/bixi/kylin/utility"
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
	sync.Mutex
	done         chan struct{}
	encoder      Encoder
	decoder      Decoder
	dispatcher   Dispatcher
	errorHandler ErrorHandler
	sendbox      utility.NonblockingChan
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
	t.sendbox = utility.NewNonblockingChan(128)
	return &t
}

func (t *transporter) Start() error {
	t.Lock()
	defer t.Unlock()
	if t.isRunning {
		return errors.New("transporter is running.")
	}

	t.isRunning = true
	t.done = make(chan struct{}, 1)

	//sending goroutine
	go func() {
		for message := range t.sendbox.In {
			err := t.encoder.Encode(message)
			if err != nil {
				t.handleError(err)
				break
			}
		}
		close(t.done)
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
	t.sendbox.Out <- message
}

func (t *transporter) Stop() error {
	t.Lock()
	defer t.Unlock()
	if t.isStopped {
		return errors.New("transporter has stopped.")
	}
	t.isStopped = true
	t.sendbox.Close()
	return nil
}

func (t *transporter) WaitForDone() {
	defer recover()
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

func (t *transporter) handleError(err error) {
	if t.errorHandler != nil {
		if shouldReport(err) {
			t.errorHandler.Handle(err)
		}
	}
}
