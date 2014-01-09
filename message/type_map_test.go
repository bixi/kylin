package message

import (
	"reflect"
	"testing"
)

type testMessage struct {
}

func TestTypeMap(t *testing.T) {
	typeMap := NewTypeMap()
	err := typeMap.Register(testMessage{})
	if err != nil {
		t.Fatal(err)
	}
	message := typeMap.Get("testMessage")
	if (reflect.TypeOf(message) != reflect.TypeOf(testMessage{})) {
		t.Fail()
	}
	typeMap.RegisterName("PointerToTestMessage", &testMessage{})
	message := typeMap.Get("PointerToTestMessage")
	if (reflect.TypeOf(message) != reflect.TypeOf(&testMessage{})) {
		t.Fail()
	}
}
