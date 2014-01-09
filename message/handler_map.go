package message

import (
	"errors"
	"log"
	"reflect"
	"unicode"
	"unicode/utf8"
)

type HandlerMap map[reflect.Type]*reflect.Method

func NewHandlerMap() HandlerMap {
	return make(map[reflect.Type]*reflect.Method)
}

func (handlerMap HandlerMap) Handle(contex interface{}, message interface{}) error {
	handler := handlerMap[reflect.TypeOf(message)]
	if handler == nil {
		return errors.New("Message handler not found:" + reflect.TypeOf(message).String() + "\n")
	}
	err := handler.Func.Call([]reflect.Value{reflect.ValueOf(contex), reflect.ValueOf(message)})
	if err[0].IsNil() {
		return nil
	} else {
		return err[0].Interface().(error)
	}
}

func (handlerMap HandlerMap) Register(contex interface{}) {
	handlerMap.register(contex, true)
}

func (handlerMap HandlerMap) RegisterWithoutLog(contex interface{}) {
	handlerMap.register(contex, false)
}

func (handlerMap HandlerMap) register(contex interface{}, printLog bool) {
	t := reflect.TypeOf(contex)
	num := 0
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		if isSuitableMethod(&method, printLog) {
			key := method.Type.In(1)
			if _, ok := handlerMap[key]; ok {
				if printLog {
					log.Printf("%v:duplicate handler of %v, override.\n", t.Name(), key.Name())
				}
			}
			handlerMap[method.Type.In(1)] = &method
			num++
		}
	}
	if printLog {
		log.Printf("%v:%v handler(s) registered.\n", t.Name(), num)
	}
}

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

func isSuitableMethod(method *reflect.Method, printLog bool) bool {
	if method.Type.PkgPath() != "" {
		if printLog {
			log.Printf("%v is not exported.", method.Name)
		}
		return false
	}
	if method.Type.NumIn() != 2 {
		if printLog {
			log.Printf("%v number of args not matched.", method.Name)
		}
		return false
	}
	argType := method.Type.In(1)
	if argType.Kind() != reflect.Ptr {
		if printLog {
			log.Printf("%v the argument must be a pointer.", method.Name)
		}
		return false
	}
	if !isExportedOrBuiltinType(argType) {
		if printLog {
			log.Printf("%v the argument must be exported.", method.Name)
		}
		return false
	}
	if method.Type.NumOut() != 1 {
		if printLog {
			log.Printf("%v number of return value must be 1.", method.Name)
		}
		return false
	}
	if returnType := method.Type.Out(0); returnType != typeOfError {
		if printLog {
			log.Printf("%v the type of return value must be error.", method.Name)
		}
		return false
	}
	return true
}
