package message

import (
	"errors"
	"reflect"
)

type TypeMap map[string]reflect.Type

func NewTypeMap() TypeMap {
	return make(TypeMap)
}

func (typeMap TypeMap) Register(value interface{}) error {
	name := reflect.TypeOf(value).Name()
	return typeMap.RegisterName(name, value)

}

func (typeMap TypeMap) RegisterName(name string, value interface{}) error {
	if _, ok := typeMap[name]; ok {
		return errors.New("TypeMap conflict:" + name)
	}
	typeMap[name] = reflect.TypeOf(value)
	return nil
}

func (typeMap TypeMap) Instantiate(name string) interface{} {
	if t, ok := typeMap[name]; ok {
		return reflect.New(t).Elem().Interface()
	}
	return nil
}
