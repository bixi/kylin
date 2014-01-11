package message

type Decoder interface {
	Decode(message interface{}) error
}

type DecoderFunc func(message interface{}) error

func (f DecoderFunc) Decode(message interface{}) error {
	return f(message)
}
