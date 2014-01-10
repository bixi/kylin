package message

type Encoder interface {
	Encode(message interface{}) error
}

type EncoderFunc func(message interface{}) error

func (f EncoderFunc) Encode(message interface{}) error {
	return f(message)
}
