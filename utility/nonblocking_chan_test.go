package utility

import (
	"testing"
)

func TestNonblockingChan(t *testing.T) {
	uc := NewNonblockingChan(10)
	for i := 0; i < 20; i++ {
		uc.Out <- i
	}
	uc.Close()
	j := 0
	for v := range uc.In {
		if v != j {
			t.Errorf("NonblockingChan error, expected:%v, receive:%v", j, v)
		}
		j++
	}
}
