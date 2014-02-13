package utility

type NonblockingChan struct {
	Out chan<- interface{}
	In  <-chan interface{}
}

func NewNonblockingChan(bufferSize int) NonblockingChan {
	uc := NonblockingChan{}
	out := make(chan interface{}, bufferSize)
	in := make(chan interface{}, bufferSize)
	uc.Out = out
	uc.In = in
	go func() {
		var closed bool
		buffer := make([]interface{}, 0, bufferSize)
		slice := buffer
		for {
			if len(slice) == 0 {
				select {
				case i, ok := <-out:
					if ok {
						slice = append(slice, i)
					} else {
						closed = true
					}
				}
			} else {
				select {
				case i, ok := <-out:
					if ok {
						slice = append(slice, i)
					} else {
						closed = true
					}
				case in <- slice[0]:
					slice = slice[1:]
				}
			}
			if len(slice) == 0 {
				slice = buffer
				if closed {
					close(in)
					return
				}
			}
		}
	}()
	return uc
}

func (uc NonblockingChan) Close() {
	close(uc.Out)
}
