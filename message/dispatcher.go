package message

type Dispatcher interface {
	Dispatch(message interface{}) error
}

type DispatcherFunc func(message interface{}) error

func (f DispatcherFunc) Dispatch(message interface{}) error {
	return f(message)
}
