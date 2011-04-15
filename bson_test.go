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
	"bytes"
	"testing"
	"reflect"
	"time"
)

func testMap(value interface{}) map[string]interface{} {
	return map[string]interface{}{"test": value}
}

type stEmpty struct{}

type stFloat64 struct {
	Test float64 "test/c"
}

type stString struct {
	Test string "test/c"
}

type stDoc struct {
	Test map[string]interface{} "test/c"
}

type stBinary struct {
	Test []byte "test/c"
}

type stObjectId struct {
	Test ObjectId "test/c"
}

type stBool struct {
	Test bool "test/c"
}

type ncBool struct {
	Test bool "test"
}

type stRegexp struct {
	Test Regexp "test/c"
}

type stSymbol struct {
	Test Symbol "test/c"
}

type stInt8 struct {
	Test int8 "test/c"
}

type stInt16 struct {
	Test int16 "test/c"
}

type stInt32 struct {
	Test int32 "test/c"
}

type stInt struct {
	Test int "test/c"
}

type stUint8 struct {
	Test uint8 "test/c"
}

type stUint16 struct {
	Test uint16 "test/c"
}

type stUint32 struct {
	Test uint32 "test/c"
}

type stUint struct {
	Test uint "test/c"
}

type stInt64 struct {
	Test int64 "test/c"
}

type stDateTime struct {
	Test DateTime "test/c"
}

type stTimestamp struct {
	Test Timestamp "test/c"
}

type stMinMax struct {
	Test MinMax "test/c"
}

type stCodeWithScope struct {
	Test CodeWithScope "test/c"
}

type stAny struct {
	Test interface{} "test/c"
}

type stStringSlice struct {
	Test []string "test/c"
}

type stStringArray struct {
	Test [1]string "test/c"
}

type stId struct {
	Id   int "_id/c"
	Test int "test/c"
}

type stEmbed struct {
	Id int "_id/c"
	stInt32
}

var bsonTests = []struct {
	psv  interface{}
	sv   interface{}
	mv   map[string]interface{}
	data string
}{
	{nil, stFloat64{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stString{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stAny{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stDoc{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stBinary{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stObjectId{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stBool{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stSymbol{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stInt8{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stInt16{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stInt32{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stUint{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stUint8{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stUint16{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stUint32{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stUint{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stInt64{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stMinMax{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stCodeWithScope{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stRegexp{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stTimestamp{}, nil, "\x05\x00\x00\x00\x00"},
	{nil, stDateTime{}, nil, "\x05\x00\x00\x00\x00"},

	{new(stEmpty), stEmpty{}, map[string]interface{}{},
		"\x05\x00\x00\x00\x00"},

	{new(stFloat64), stFloat64{1.5}, testMap(float64(1.5)),
		"\x13\x00\x00\x00\x01test\x00\x00\x00\x00\x00\x00\x00\xf8?\x00"},

	{new(stString), stString{"world"}, testMap("world"),
		"\x15\x00\x00\x00\x02test\x00\x06\x00\x00\x00world\x00\x00"},

	{new(stAny), stAny{"world"}, testMap("world"),
		"\x15\x00\x00\x00\x02test\x00\x06\x00\x00\x00world\x00\x00"},

	{new(stDoc), stDoc{make(map[string]interface{})}, testMap(make(map[string]interface{})),
		"\x10\x00\x00\x00\x03test\x00\x05\x00\x00\x00\x00\x00"},

	{new(stBinary), stBinary{[]byte("test")}, testMap([]byte("test")),
		"\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00\x00\x00\x74\x65\x73\x74\x00"},

	{new(stObjectId), stObjectId{ObjectId("\x4C\x9B\x8F\xB4\xA3\x82\xAA\xFE\x17\xC8\x6E\x63")},
		testMap(ObjectId("\x4C\x9B\x8F\xB4\xA3\x82\xAA\xFE\x17\xC8\x6E\x63")),
		"\x17\x00\x00\x00\x07test\x00\x4C\x9B\x8F\xB4\xA3\x82\xAA\xFE\x17\xC8\x6E\x63\x00"},
	{new(stObjectId), stObjectId{ObjectId("")},
		map[string]interface{}{},
		"\x05\x00\x00\x00\x00"},

	{new(ncBool), ncBool{true}, testMap(true),
		"\x0C\x00\x00\x00\x08test\x00\x01\x00"},
	{new(ncBool), ncBool{false}, testMap(false),
		"\x0C\x00\x00\x00\x08test\x00\x00\x00"},

	{new(stSymbol), stSymbol{Symbol("aSymbol")}, testMap(Symbol("aSymbol")),
		"\x17\x00\x00\x00\x0Etest\x00\x08\x00\x00\x00aSymbol\x00\x00"},

	{nil, stInt8{10}, testMap(int8(10)),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},
	{nil, stInt16{10}, testMap(int16(10)),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},
	{nil, stInt32{10}, testMap(int32(10)),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},
	{new(stInt), stInt{10}, testMap(10),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},

	{nil, stUint8{10}, testMap(uint8(10)),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},
	{nil, stUint16{10}, testMap(uint16(10)),
		"\x0F\x00\x00\x00\x10test\x00\x0A\x00\x00\x00\x00"},

	{nil, stUint32{256}, testMap(uint32(256)),
		"\x13\x00\x00\x00\x12test\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"},
	{nil, stUint{256}, testMap(uint(256)),
		"\x13\x00\x00\x00\x12test\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"},

	{new(stInt64), stInt64{256}, testMap(int64(256)),
		"\x13\x00\x00\x00\x12test\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"},

	{new(stMinMax), stMinMax{MaxValue}, testMap(MaxValue),
		"\x0B\x00\x00\x00\x7Ftest\x00\x00"},
	{new(stMinMax), stMinMax{MinValue}, testMap(MinValue),
		"\x0B\x00\x00\x00\xFFtest\x00\x00"},

	{nil, stRegexp{Regexp{"a*b", "i"}}, testMap(Regexp{"a*b", "i"}),
		"\x11\x00\x00\x00\vtest\x00a*b\x00i\x00\x00"},

	{nil, stCodeWithScope{CodeWithScope{"test", nil}}, testMap(CodeWithScope{"test", nil}),
		"\x1d\x00\x00\x00\x0ftest\x00\x12\x00\x00\x00\x05\x00\x00\x00test\x00\x05\x00\x00\x00\x00\x00"},

	{new(stTimestamp), stTimestamp{1168216211000}, testMap(Timestamp(1168216211000)),
		"\x13\x00\x00\x00\x11test\x008\xbe\x1c\xff\x0f\x01\x00\x00\x00"},

	{new(stDateTime), stDateTime{1168216211000}, testMap(DateTime(1168216211000)),
		"\x13\x00\x00\x00\ttest\x008\xbe\x1c\xff\x0f\x01\x00\x00\x00"},

	{new(stStringSlice), stStringSlice{[]string{}}, testMap([]interface{}{}),
		"\x10\x00\x00\x00\x04test\x00\x05\x00\x00\x00\x00\x00"},

	{new(stStringSlice), stStringSlice{[]string{"hello"}}, testMap([]interface{}{"hello"}),
		"\x1d\x00\x00\x00\x04test\x00\x12\x00\x00\x00\x020\x00\x06\x00\x00\x00hello\x00\x00\x00"},

	{new(stStringArray), stStringArray{[1]string{"hello"}}, testMap([]interface{}{"hello"}),
		"\x1d\x00\x00\x00\x04test\x00\x12\x00\x00\x00\x020\x00\x06\x00\x00\x00hello\x00\x00\x00"},

	{new(BSONData), BSONData{Kind: kindDocument, Data: []byte("\x15\x00\x00\x00\x02test\x00\x06\x00\x00\x00world\x00\x00")},
		testMap("world"),
		"\x15\x00\x00\x00\x02test\x00\x06\x00\x00\x00world\x00\x00"},

	{new(stId), stId{Test: 2, Id: 1}, map[string]interface{}{"test": 2, "_id": 1},
		"\x18\x00\x00\x00\x10_id\x00\x01\x00\x00\x00\x10test\x00\x02\x00\x00\x00\x00"},

	{new(stEmbed), stEmbed{stInt32: stInt32{2}, Id: 1}, map[string]interface{}{"test": 2, "_id": 1},
		"\x18\x00\x00\x00\x10_id\x00\x01\x00\x00\x00\x10test\x00\x02\x00\x00\x00\x00"},
}

func TestEncodeMap(t *testing.T) {
	for _, bt := range bsonTests {
		if bt.mv == nil {
			continue
		}
		var data []byte
		data, err := Encode(data, bt.mv)
		if err != nil {
			t.Errorf("Encode(data, %q) returned error %v", bt.mv, err)
		} else if string(data) != bt.data {
			t.Errorf("Encode(data, %q) data=%q, want %q", bt.mv, string(data), bt.data)
		}
	}
}

func TestEncodeStruct(t *testing.T) {
	for _, bt := range bsonTests {
		var data []byte
		data, err := Encode(data, bt.sv)
		if err != nil {
			t.Errorf("Encoded(data, %q) returned error %v", bt.sv, err)
		} else if string(data) != bt.data {
			t.Errorf("Encoded(data, %q) data=%q, want %q", bt.sv, string(data), bt.data)
		}
	}
}

func TestDecodeMap(t *testing.T) {
	for _, bt := range bsonTests {
		if bt.psv == nil {
			continue
		}
		m := map[string]interface{}{}
		err := Decode([]byte(bt.data), m)
		if err != nil {
			t.Errorf("Decode(%q, m) returned error %v", bt.data, err)
		} else if !reflect.DeepEqual(bt.mv, m) {
			t.Errorf("Decode(%q, m) m = %q, want %q", bt.data, m, bt.mv)
		}
	}
}

func TestDecodeMapPtr(t *testing.T) {
	for _, bt := range bsonTests {
		if bt.psv == nil {
			continue
		}
		var m map[string]interface{}
		err := Decode([]byte(bt.data), &m)
		if err != nil {
			t.Errorf("Decode(%q, m) returned error %v", bt.data, err)
		} else if !reflect.DeepEqual(bt.mv, m) {
			t.Errorf("Decode(%q, m) m = %q, want %q", bt.data, m, bt.mv)
		}
	}
}

func TestDecodeStruct(t *testing.T) {
	for _, bt := range bsonTests {
		if bt.psv == nil {
			continue
		}
		pt := reflect.NewValue(bt.psv).Type()
		psv := reflect.Zero(pt)
		psv.Set(reflect.Zero(pt.Elem()).Addr())
		err := Decode([]byte(bt.data), psv.Interface())
		sv := psv.Elem().Interface()
		if err != nil {
			t.Errorf("Decode(%q, sv) returned error %v", bt.data, err)
		} else if !reflect.DeepEqual(sv, bt.sv) {
			t.Errorf("Decode(%q, sv) sv = %q, want %q", bt.data, sv, bt.sv)
		}
	}
}

func TestEncodeOrderedMap(t *testing.T) {
	m := D{{"test", "hello world"}}
	expected := []byte("\x1b\x00\x00\x00\x02test\x00\f\x00\x00\x00hello world\x00\x00")
	var actual []byte
	actual, err := Encode(actual, m)
	if err != nil {
		t.Error("error encoding map %s", err)
	} else if !bytes.Equal(expected, actual) {
		t.Errorf("  expected %q\n  actual   %q", expected, actual)
	}
}

func TestEncodeOrderedMapOld(t *testing.T) {
	m := Doc{{"test", "hello world"}}
	expected := []byte("\x1b\x00\x00\x00\x02test\x00\f\x00\x00\x00hello world\x00\x00")
	var actual []byte
	actual, err := Encode(actual, m)
	if err != nil {
		t.Error("error encoding map %s", err)
	} else if !bytes.Equal(expected, actual) {
		t.Errorf("  expected %q\n  actual   %q", expected, actual)
	}
}

func TestObjectId(t *testing.T) {
	t1 := time.Seconds()
	min := MinObjectIdForTime(t1)
	id := NewObjectId()
	max := MaxObjectIdForTime(time.Seconds())
	if id < min {
		t.Errorf("%q < %q", id, min)
	}
	if id > max {
		t.Errorf("%q > %q", id, max)
	}
	if min.CreationTime() != t1 {
		t.Errorf("min.CreationTime() = %d, want %d", min.CreationTime(), t1)
	}
	id2, err := NewObjectIdHex(id.String())
	if err != nil {
		t.Errorf("NewObjectIdString returned %q", err)
	}
	if id2 != id {
		t.Errorf("%q != %q", id2, id)
	}
	t2 := ObjectId("").CreationTime()
	if t2 != 0 {
		t.Error("creation time for invalid id = %d, want 0", t1)
	}
}

func TestBadDecodeResults(t *testing.T) {
	empty := []byte("\x05\x00\x00\x00\x00")

	var m M
	err := Decode(empty, m)
	if err == nil {
		t.Error("Decode nil map did not return an error.")
	}

	err = Decode(empty, struct{}{})
	if err == nil {
		t.Error("Decode struct value did not return an error.")
	}

	var p *struct{}
	err = Decode(empty, p)
	if err == nil {
		t.Error("Decode nil pointer did not return an error.")
	}

	err = Decode(empty, 1)
	if err == nil {
		t.Error("Decode int did not return an error.")
	}

	err = Decode(empty, new(int))
	if err == nil {
		t.Error("Decode *int did not return an error.")
	}
}

var structFieldsTests = []struct {
	v interface{}
	m M
}{
	{
		struct{}{},
		M{"_id": 0},
	},
	{
		struct {
			Id   int "_id"
			Test int
		}{},
		M{"Test": 1},
	},
}

func TestStructFields(t *testing.T) {
	for _, tt := range structFieldsTests {
		fields := StructFields(reflect.NewValue(tt.v).Type())
		m := make(M)
		for _, di := range fields.(D) {
			m[di.Key] = di.Value
		}
		if !reflect.DeepEqual(m, tt.m) {
			t.Errorf("%+v fields=%v, want %v\n", tt.v, m, tt.m)
		}
	}
}
