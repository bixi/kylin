package message

type ErrorHandler interface {
	OnError(error)
}

type ErrorHandlerFunc func(error)

func (f ErrorHandlerFunc) OnError(err error) {
	f(err)
}
