package message

type Decoder interface {
	Decode() (message interface{}, err error)
}

type DecoderFunc func() (message interface{}, err error)

func (f DecoderFunc) Decode() (message interface{}, err error) {
	return f()
}
