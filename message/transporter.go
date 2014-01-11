package message

import (
	"errors"
	"io"
	"strings"
	"sync"
)

type Transporter struct {
	Done         <-chan bool
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
	errorHandler ErrorHandler) *Transporter {
	var transporter Transporter
	transporter.encoder = encoder
	transporter.decoder = decoder
	transporter.dispatcher = dispatcher
	transporter.errorHandler = errorHandler
	transporter.sendbox = make(chan []interface{}, 1)
	return &transporter
}

func (transporter *Transporter) Start() error {
	transporter.mutex.Lock()
	defer transporter.mutex.Unlock()
	if transporter.isRunning {
		return errors.New("Transporter is running.")
	}

	transporter.isRunning = true
	done := make(chan bool, 1)
	transporter.Done = done

	//sending goroutine
	go func() {
		for messages := range transporter.sendbox {
			err := transporter.processSendingMessages(messages)
			if err != nil {
				transporter.handleError(err)
				break
			}
		}
		done <- true
	}()

	//receving goroutine
	go func() {
		for {
			var message interface{}
			err := transporter.decoder.Decode(&message)
			if err == nil {
				transporter.dispatcher.Dispatch(message)
			} else {
				transporter.handleError(err)
				transporter.Stop()
				return
			}
		}
	}()

	return nil
}

func (transporter *Transporter) Send(message interface{}) {
	transporter.mutex.Lock()
	defer transporter.mutex.Unlock()
	select {
	case messages := <-transporter.sendbox:
		messages = append(messages, message)
		transporter.sendbox <- messages
	default:
		messages := []interface{}{message}
		transporter.sendbox <- messages
	}
}

func (transporter *Transporter) Stop() error {
	transporter.mutex.Lock()
	defer transporter.mutex.Unlock()
	if transporter.isStopped {
		return errors.New("Transporter has stopped.")
	}
	transporter.isStopped = true
	close(transporter.sendbox)
	return nil
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

func (transporter *Transporter) processSendingMessages(messages []interface{}) error {
	for _, message := range messages {
		err := transporter.encoder.Encode(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (transporter *Transporter) handleError(err error) {
	if transporter.errorHandler != nil {
		if shouldReport(err) {
			transporter.errorHandler.Handle(err)
		}
	}
}
