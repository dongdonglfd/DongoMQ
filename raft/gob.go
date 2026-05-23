package raft

import (
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
)

var mu sync.Mutex
var errorCount int
var checked map[reflect.Type]bool

type LabEncoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *LabEncoder {
	enc := &LabEncoder{
		gob: gob.NewEncoder(w),
	}
	return enc
}

func (enc *LabEncoder) Encode(v interface{}) error {
	checkValue(v)
	return enc.gob.Encode(v)
}

func (enc *LabEncoder) EncodeValue(v reflect.Value) error {
	checkValue(v.Interface())
	return enc.gob.EncodeValue(v)
}

type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	dec := &LabDecoder{
		gob: gob.NewDecoder(r),
	}
	return dec
}

func (dec *LabDecoder) Decode(v interface{}) error {
	checkValue(v)
	checkDefault(v)
	return dec.gob.Decode(v)
}

func Register(v interface{}) {
	checkValue(v)
	gob.Register(v)
}

func RegisterName(name string, v interface{}) {
	checkValue(v)
	gob.RegisterName(name, v)
}

func checkValue(v interface{}) {
	checkType(reflect.TypeOf(v))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	mu.Lock()
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if !unicode.IsUpper(rune) {
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your raft\n", f.Name, t.Name())
				mu.Lock()
				errorCount++
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Key())
		checkType(t.Elem())
		return
	default:
		return
	}
}

func checkDefault(v interface{}) {
	if v == nil {
		return
	}
	checkDefault1(reflect.ValueOf(v), 1, "")
}

func checkDefault1(v reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := v.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := v.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefault1(vv, depth+1, name1)
		}
		return
	case reflect.Ptr:
		if v.IsNil() {
			return
		}
		checkDefault1(v.Elem(), depth+1, name)
		return
	case reflect.Bool, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.String:
		if reflect.DeepEqual(reflect.Zero(t).Interface(), v.Interface()) == false {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				fmt.Printf("labgob warning: Decoding into a non-default variable/field %v may not work\n", what)
			}
			errorCount++
			mu.Unlock()
		}
		return
	}
}
