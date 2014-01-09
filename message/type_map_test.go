package message

import (
	"reflect"
	"testing"
)

func TestTypeMapRigister(t *testing.T) {
	typeMap := NewTypeMap()
	var hello string = "hello"
	err := typeMap.Register(hello)
	if err != nil {
		t.Fatal(err)
	}
	message := typeMap.Instantiate("string")
	if reflect.TypeOf(message) != reflect.TypeOf(hello) {
		t.Fail()
	}

}

func TestTypeMapRigisterName(t *testing.T) {
	typeMap := NewTypeMap()
	var hello string = "hello"
	typeMap.RegisterName("PointerToString", &hello)
	message := typeMap.Instantiate("PointerToString")
	if reflect.TypeOf(message) != reflect.TypeOf(&hello) {
		t.Fail()
	}
}
