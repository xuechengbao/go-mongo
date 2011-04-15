// Copyright 2010 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
)

var (
	typeD        = reflect.Typeof(D{})
	typeDoc      = reflect.Typeof(Doc{})
	typeBSONData = reflect.Typeof(BSONData{})
	idKey        = reflect.NewValue("_id")
)

// EncodeTypeError is the error indicating that Encode could not encode an input type.
type EncodeTypeError struct {
	Type reflect.Type
}

func (e *EncodeTypeError) String() string {
	return "bson: unsupported type: " + e.Type.String()
}

type encodeState struct {
	buffer
}

// Encode appends the BSON encoding of doc to buf and returns the new slice.
//
// Encode traverses the value doc recursively using the following
// type-dependent encodings:
//
// Struct values encode as BSON documents. The struct field tag specifies the
// encoded name of the field and encoding options. The options follow the name
// and are proceeded by a '/'.  If the name is not specified in the tag, then
// the field name defaults to the structure field name. Unexported fields and
// fields equal to nil are note encoded.  The following option is supported:
//
//  /c  If the field is the zero value, then the field is not 
//      written to the encoding. 
//
// Array and slice values encode as BSON arrays.
//
// Map values encode as BSON documents. The map's key type must be string; the
// object keys are used directly as map keys.
//
// Pointer values encode as the value pointed to. 
//
// Interface values encode as the value contained in the interface. 
// 
// Other types are encoded as follows
//
//      Go                  -> BSON
//      bool                -> Boolean
//      float32             -> Double
//      float64             -> Double
//      int                 -> 32-bit Integer
//      int8, int16, int32  -> 32-bit Integer
//      int64               -> 64-bit Integer
//      uint8, uint16       -> 32-bit Integer
//      uint, uint32        -> 64-bit Integer
//      string              -> String
//      []byte              -> Binary data
//      mongo.Code          -> Javascript code
//      mongo.CodeWithScope -> Javascript code with scope
//      mongo.DateTime      -> UTC Datetime
//      mongo.D             -> Document. Use when element order is important.
//      mongo.MinMax        -> Minimum / Maximum value
//      mongo.ObjectId      -> ObjectId
//      mongo.Regexp        -> Regular expression
//      mongo.Symbol        -> Symbol
//      mongo.Timestamp     -> Timestamp
//
// Other types including channels, complex and function values cannot be encoded.
//
// BSON cannot represent cyclic data structure and Encode does not handle them.
// Passing cyclic structures to Encode will result in an infinite recursion.
func Encode(buf []byte, doc interface{}) (result []byte, err os.Error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(os.Error)
		}
	}()

	v := reflect.NewValue(doc)
	if kind := v.Kind(); kind == reflect.Interface || kind == reflect.Ptr {
		v = v.Elem()
	}

	e := encodeState{buffer: buf}
	switch v.Type() {
	case typeD:
		e.writeD(v.Interface().(D))
	case typeDoc:
		e.writeDoc(v.Interface().(Doc))
	case typeBSONData:
		rd := v.Interface().(BSONData)
		if rd.Kind != kindDocument {
			return nil, &EncodeTypeError{v.Type()}
		}
		e.Write(rd.Data)
	default:
		switch v.Kind() {
		case reflect.Struct:
			e.writeStruct(v)
		case reflect.Map:
			e.writeMap(v, true)
		default:
			return nil, &EncodeTypeError{v.Type()}
		}
	}
	return e.buffer, nil
}

func (e *encodeState) abort(err os.Error) {
	panic(err)
}

func (e *encodeState) beginDoc() (offset int) {
	offset = len(e.buffer)
	e.buffer.Next(4)
	return
}

func (e *encodeState) endDoc(offset int) {
	n := len(e.buffer) - offset
	wire.PutUint32(e.buffer[offset:offset+4], uint32(n))
}

func (e *encodeState) writeKindName(kind int, name string) {
	e.WriteByte(byte(kind))
	e.WriteCString(name)
}

func (e *encodeState) writeStruct(v reflect.Value) {
	offset := e.beginDoc()
	si := structInfoForType(v.Type())
	for _, fi := range si.l {
		e.encodeValue(fi.name, fi, v.FieldByIndex(fi.index))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeMap(v reflect.Value, topLevel bool) {
	if v.IsNil() {
		return
	}
	if v.Type().Key().Kind() != reflect.String {
		e.abort(&EncodeTypeError{v.Type()})
	}
	offset := e.beginDoc()
	skipId := false
	if topLevel {
		idValue := v.MapIndex(idKey)
		if idValue.IsValid() {
			skipId = true
			e.encodeValue("_id", defaultFieldInfo, idValue)
		}
	}
	for _, k := range v.MapKeys() {
		sk := k.String()
		if !skipId || sk != "_id" {
			e.encodeValue(sk, defaultFieldInfo, v.MapIndex(k))
		}
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeD(v D) {
	offset := e.beginDoc()
	for _, kv := range v {
		e.encodeValue(kv.Key, defaultFieldInfo, reflect.NewValue(kv.Value))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeDoc(v Doc) {
	offset := e.beginDoc()
	for _, kv := range v {
		e.encodeValue(kv.Key, defaultFieldInfo, reflect.NewValue(kv.Value))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) encodeValue(name string, fi *fieldInfo, v reflect.Value) {
	if !v.IsValid() {
		return
	}
	t := v.Type()
	encoder, found := typeEncoder[t]
	if !found {
		encoder, found = kindEncoder[t.Kind()]
		if !found {
			e.abort(&EncodeTypeError{t})
		}
	}
	encoder(e, name, fi, v)
}

func encodeBool(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	b := v.Bool()
	if b == false && fi.conditional {
		return
	}
	e.writeKindName(kindBool, name)
	if b {
		e.WriteByte(1)
	} else {
		e.WriteByte(0)
	}
}

func encodeInt32(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	i := v.Int()
	if i == 0 && fi.conditional {
		return
	}
	e.writeKindName(kindInt32, name)
	e.WriteUint32(uint32(i))
}

func encodeUint32(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	i := v.Uint()
	if i == 0 && fi.conditional {
		return
	}
	e.writeKindName(kindInt32, name)
	e.WriteUint32(uint32(i))
}

func encodeInt64(e *encodeState, kind int, name string, fi *fieldInfo, v reflect.Value) {
	i := v.Int()
	if i == 0 && fi.conditional {
		return
	}
	e.writeKindName(kind, name)
	e.WriteUint64(uint64(i))
}

func encodeUint64(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	i := v.Uint()
	if i == 0 && fi.conditional {
		return
	}
	e.writeKindName(kindInt64, name)
	e.WriteUint64(i)
}

func encodeFloat(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	f := v.Float()
	if f == 0 && fi.conditional {
		return
	}
	e.writeKindName(kindFloat, name)
	e.WriteUint64(math.Float64bits(f))
}

func encodeString(e *encodeState, kind int, name string, fi *fieldInfo, v reflect.Value) {
	s := v.String()
	if s == "" && fi.conditional {
		return
	}
	e.writeKindName(kind, name)
	e.WriteUint32(uint32(len(s) + 1))
	e.WriteCString(s)
}

func encodeRegexp(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	r := v.Interface().(Regexp)
	if r.Pattern == "" && fi.conditional {
		return
	}
	e.writeKindName(kindRegexp, name)
	e.WriteCString(r.Pattern)
	e.WriteCString(r.Options)
}

func encodeObjectId(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	oid := v.Interface().(ObjectId)
	if oid == "" {
		return
	}
	if len(oid) != 12 {
		e.abort(os.NewError("bson: object id length != 12"))
	}
	e.writeKindName(kindObjectId, name)
	copy(e.Next(12), oid)
}

func encodeBSONData(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	bd := v.Interface().(BSONData)
	if bd.Kind == 0 {
		return
	}
	e.writeKindName(bd.Kind, name)
	e.Write(bd.Data)
}

func encodeCodeWithScope(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	c := v.Interface().(CodeWithScope)
	if c.Code == "" && c.Scope == nil && fi.conditional {
		return
	}
	e.writeKindName(kindCodeWithScope, name)
	offset := e.beginDoc()
	e.WriteUint32(uint32(len(c.Code) + 1))
	e.WriteCString(c.Code)
	scopeOffset := e.beginDoc()
	for k, v := range c.Scope {
		e.encodeValue(k, defaultFieldInfo, reflect.NewValue(v))
	}
	e.WriteByte(0)
	e.endDoc(scopeOffset)
	e.endDoc(offset)
}

func encodeMinMax(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	i := v.Interface().(MinMax)
	if i == 0 && fi.conditional {
		return
	}
	switch v.Interface().(MinMax) {
	case 1:
		e.writeKindName(kindMaxValue, name)
	case -1:
		e.writeKindName(kindMinValue, name)
	default:
		e.abort(os.NewError("bson: unknown MinMax value"))
	}
}

func encodeStruct(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	e.writeKindName(kindDocument, name)
	e.writeStruct(v)
}

func encodeMap(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	if v.IsNil() {
		return
	}
	e.writeKindName(kindDocument, name)
	e.writeMap(v, false)
}

func encodeD(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	d := v.Interface().(D)
	if d == nil {
		return
	}
	e.writeKindName(kindDocument, name)
	e.writeD(d)
}

func encodeDoc(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	d := v.Interface().(Doc)
	if d == nil {
		return
	}
	e.writeKindName(kindDocument, name)
	e.writeDoc(d)
}

func encodeByteSlice(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	b := v.Interface().([]byte)
	if b == nil {
		return
	}
	e.writeKindName(kindBinary, name)
	e.WriteUint32(uint32(len(b)))
	e.WriteByte(0)
	e.Write(b)
}

func encodeSlice(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	if v.IsNil() {
		return
	}
	e.writeKindName(kindArray, name)
	offset := e.beginDoc()
	n := v.Len()
	for i := 0; i < n; i++ {
		e.encodeValue(strconv.Itoa(i), defaultFieldInfo, v.Index(i))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func encodeArray(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	e.writeKindName(kindArray, name)
	offset := e.beginDoc()
	n := v.Len()
	for i := 0; i < n; i++ {
		e.encodeValue(strconv.Itoa(i), defaultFieldInfo, v.Index(i))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func encodeInterfaceOrPtr(e *encodeState, name string, fi *fieldInfo, v reflect.Value) {
	if v.IsNil() {
		return
	} else {
		e.encodeValue(name, defaultFieldInfo, v.Elem())
	}
}

type encoderFunc func(e *encodeState, name string, fi *fieldInfo, v reflect.Value)

var kindEncoder map[reflect.Kind]encoderFunc
var typeEncoder map[reflect.Type]encoderFunc

func init() {
	kindEncoder = map[reflect.Kind]encoderFunc{
		reflect.Array:   encodeArray,
		reflect.Bool:    encodeBool,
		reflect.Float32: encodeFloat,
		reflect.Float64: encodeFloat,
		reflect.Int8:    encodeInt32,
		reflect.Int16:   encodeInt32,
		reflect.Int32:   encodeInt32,
		reflect.Int:     encodeInt32,
		reflect.Uint8:   encodeUint32,
		reflect.Uint16:  encodeUint32,
		reflect.Uint32:  encodeUint64,
		reflect.Uint:    encodeUint64,
		reflect.Int64: func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeInt64(e, kindInt64, name, fi, value)
		},
		reflect.Interface: encodeInterfaceOrPtr,
		reflect.Map:       encodeMap,
		reflect.Ptr:       encodeInterfaceOrPtr,
		reflect.Slice:     encodeSlice,
		reflect.String: func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeString(e, kindString, name, fi, value)
		},
		reflect.Struct: encodeStruct,
	}
	typeEncoder = map[reflect.Type]encoderFunc{
		typeDoc:      encodeDoc,
		typeD:        encodeD,
		typeBSONData: encodeBSONData,
		reflect.Typeof(Code("")): func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeString(e, kindCode, name, fi, value)
		},
		reflect.Typeof(CodeWithScope{}): encodeCodeWithScope,
		reflect.Typeof(DateTime(0)): func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeInt64(e, kindDateTime, name, fi, value)
		},
		reflect.Typeof(MinMax(0)):    encodeMinMax,
		reflect.Typeof(ObjectId("")): encodeObjectId,
		reflect.Typeof(Regexp{}):     encodeRegexp,
		reflect.Typeof(Symbol("")): func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeString(e, kindSymbol, name, fi, value)
		},
		reflect.Typeof(Timestamp(0)): func(e *encodeState, name string, fi *fieldInfo, value reflect.Value) {
			encodeInt64(e, kindTimestamp, name, fi, value)
		},
		reflect.Typeof([]byte{}): encodeByteSlice,
	}
}
