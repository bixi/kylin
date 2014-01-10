package message

type ErrorHandler interface {
	Handle(error)
}

type ErrorHandlerFunc func(error)

func (f ErrorHandlerFunc) Handle(err error) {
	f(err)
}
