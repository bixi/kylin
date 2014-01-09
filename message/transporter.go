package message

import (
	"errors"
	"io"
	"strings"
	"sync"
)

type Encoder func(interface{}) error
type Decoder func() (interface{}, error)
type MessageHandler func(interface{})
type ErrorHandler func(error)

type Transporter struct {
	Done           <-chan bool
	mutex          sync.Mutex
	encoder        Encoder
	decoder        Decoder
	messageHandler MessageHandler
	errorHandler   ErrorHandler
	sendbox        chan []interface{}
	isRunning      bool
	isStopped      bool
}

func NewTransporter(encoder Encoder,
	decoder Decoder,
	messageHandler MessageHandler,
	errorHandler ErrorHandler) *Transporter {
	var transporter Transporter
	transporter.encoder = encoder
	transporter.decoder = decoder
	transporter.messageHandler = messageHandler
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
	done := make(chan bool)
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
			message, err := transporter.decoder()
			if err == nil {
				transporter.messageHandler(message)
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

func (transporter *Transporter) Stop() {
	transporter.mutex.Lock()
	defer transporter.mutex.Unlock()
	if transporter.isStopped {
		return
	}
	transporter.isStopped = true
	close(transporter.sendbox)
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
		err := transporter.encoder(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (transporter *Transporter) handleError(err error) {
	if transporter.errorHandler != nil {
		if shouldReport(err) {
			transporter.errorHandler(err)
		}
	}
}
