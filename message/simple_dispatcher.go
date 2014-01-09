package message

type SimpleDispatcher struct {
	handlerMap HandlerMap
	contex     interface{}
}

func NewSimpleDispatcher(contex interface{}, printLog bool) *SimpleDispatcher {
	dispatcher := &SimpleDispatcher{NewHandlerMap(), contex}
	dispatcher.handlerMap.register(contex, printLog)
	return dispatcher
}

func (dispatcher *SimpleDispatcher) Dispatch(message interface{}) error {
	return dispatcher.handlerMap.Handle(dispatcher.contex, message)
}
