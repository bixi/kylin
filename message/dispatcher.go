package message

type Dispatcher interface {
	Dispatch(message interface{}) error
}
