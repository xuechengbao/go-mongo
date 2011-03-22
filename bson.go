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
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strconv"
	"sync"
	"time"
	"strings"
	"os"
)

// DateTime represents a BSON datetime. The value is in milliseconds since the
// Unix epoch.
type DateTime int64

// Timestamp represents a BSON timesamp.
type Timestamp int64

// CodeWithScope represents javascript in BSON.
type CodeWithScope struct {
	Code  string
	Scope map[string]interface{}
}

// Regexp represents a BSON regular expression.
type Regexp struct {
	Pattern string
	// The valid options are:
	//	i	Case insensitive matching
	//	l	Make \w, \W, etc. locale-dependent
	//	m	Multiline matching
	//	s	Dotall mode
	//	u	Make \w, \W, etc. match Unicode
	//	x	Verbose mode
	// Options must be specified in alphabetical order.
	Options string
}

// ObjectId represents a BSON object identifier. 
type ObjectId string


func (id ObjectId) String() string {
	return hex.EncodeToString([]byte(string(id)))
}

func newObjectId(t int64, c uint64) ObjectId {
	b := [12]byte{
		byte(t >> 24),
		byte(t >> 16),
		byte(t >> 8),
		byte(t),
		byte(c >> 56),
		byte(c >> 48),
		byte(c >> 40),
		byte(c >> 32),
		byte(c >> 24),
		byte(c >> 16),
		byte(c >> 8),
		byte(c)}
	return ObjectId(b[:])
}

// NewObjectId returns a new object id. This function uses the following format
// for object ids:
//
//  [0:4]  Big endian time since epoch in seconds. This is compatible 
//         with other drivers.
// 
//  [4:12] Incrementing counter initialized with crypto random
//         number. This ensures that object ids are unique, but
//         is simpler than the format used by other drivers.
func NewObjectId() ObjectId {
	return newObjectId(time.Seconds(), nextOidCounter())
}

// NewObjectIdString returns an object id initialized from the hexadecimal
// encoding of the object id.
func NewObjectIdString(hexString string) (ObjectId, os.Error) {
	p, err := hex.DecodeString(hexString)
	if err != nil {
		return "", err
	}
	if len(p) != 12 {
		return "", os.NewError("mongo: bad object id string len")
	}
	return ObjectId(p), nil
}

// MaxObjectIdForTime returns the maximum object id for time t in seconds from
// the epoch.
func MaxObjectIdForTime(t int64) ObjectId {
	return newObjectId(t, 0xffffffffffffffff)
}

// MinObjectIdForTime returns the minimum object id for time t in seconds from
// the epoch.
func MinObjectIdForTime(t int64) ObjectId {
	return newObjectId(t, 0)
}

// CreationTime extracts the time the object id was created in seconds since the epoch.
func (id ObjectId) CreationTime() int64 {
	return int64(id[0])<<24 + int64(id[1])<<16 + int64(id[2])<<8 + int64(id[3])
}

var (
	oidLock    sync.Mutex
	oidCounter uint64
)

func nextOidCounter() uint64 {
	oidLock.Lock()
	defer oidLock.Unlock()
	if oidCounter == 0 {
		if err := binary.Read(rand.Reader, binary.BigEndian, &oidCounter); err != nil {
			panic(err)
		}
	}
	oidCounter += 1
	return oidCounter
}

// BSONData represents a chunk of uninterpreted BSON data. Use this type to
// copy raw data into or out of a BSON encoding.
type BSONData struct {
	Kind int
	Data []byte
}

// Symbol represents a BSON symbol.
type Symbol string

// Code represents Javascript code in BSON.
type Code string

type DocItem struct {
	Key   string
	Value interface{}
}

// Doc represents a BSON document. Use Doc when the order of the key-value
// pairs is important.
type Doc []DocItem

// Append adds an item to doc.
func (d *Doc) Append(name string, value interface{}) {
	*d = append(*d, DocItem{name, value})
}

// MinMax represents either a minimum or maximum BSON value.
type MinMax int

const (
	// MaxValue is the maximum BSON value.
	MaxValue MinMax = 1
	// MinValue is the minimum BSON value.
	MinValue MinMax = -1
)

const (
	kindFloat         = 0x1
	kindString        = 0x2
	kindDocument      = 0x3
	kindArray         = 0x4
	kindBinary        = 0x5
	kindObjectId      = 0x7
	kindBool          = 0x8
	kindDateTime      = 0x9
	kindNull          = 0xA
	kindRegexp        = 0xB
	kindCode          = 0xD
	kindSymbol        = 0xE
	kindCodeWithScope = 0xF
	kindInt32         = 0x10
	kindTimestamp     = 0x11
	kindInt64         = 0x12
	kindMinValue      = 0xff
	kindMaxValue      = 0x7f
)

var kindNames = map[int]string{
	kindFloat:         "float",
	kindString:        "string",
	kindDocument:      "document",
	kindArray:         "array",
	kindBinary:        "binary",
	kindObjectId:      "objectId",
	kindBool:          "bool",
	kindDateTime:      "dateTime",
	kindNull:          "null",
	kindRegexp:        "regexp",
	kindCode:          "code",
	kindSymbol:        "symbol",
	kindCodeWithScope: "codeWithScope",
	kindInt32:         "int32",
	kindTimestamp:     "timestamp",
	kindInt64:         "int64",
	kindMinValue:      "minValue",
	kindMaxValue:      "maxValue",
}

func kindName(kind int) string {
	name, ok := kindNames[kind]
	if !ok {
		name = strconv.Itoa(kind)
	}
	return name
}

type fieldInfo struct {
	name        string
	index       []int
	conditional bool
}

type structInfo struct {
	m map[string]*fieldInfo
	l []*fieldInfo
}

func compileStructInfo(t *reflect.StructType, depth map[string]int, index []int, si *structInfo) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch {
		case f.PkgPath != "":
			// Ignore unexported fields.
		case f.Anonymous:
			// TODO: Handle pointers. Requires change to decoder and 
			// protection against infinite recursion.
			if t, ok := f.Type.(*reflect.StructType); ok {
				compileStructInfo(t, depth, append(index, i), si)
			}
		default:
			fi := &fieldInfo{name: f.Name}
			p := strings.Split(f.Tag, "/", -1)
			if len(p) > 0 {
				if len(p[0]) > 0 {
					fi.name = p[0]
				}
				for _, s := range p[1:] {
					switch s {
					case "c":
						fi.conditional = true
					default:
						panic(os.NewError("bson: unknown field flag " + s + " for type " + t.Name()))
					}
				}
			}
			d, found := depth[fi.name]
			if !found {
				d = 1 << 30
			}
			switch {
			case len(index) == d:
				// At same depth, remove from result.
				si.m[fi.name] = nil, false
				j := 0
				for i := 0; i < len(si.l); i++ {
					if fi.name != si.l[i].name {
						si.l[j] = si.l[i]
						j += 1
					}
				}
				si.l = si.l[:j]
			case len(index) < d:
				fi.index = make([]int, len(index)+1)
				copy(fi.index, index)
				fi.index[len(index)] = i
				depth[fi.name] = len(index)
				si.m[fi.name] = fi
				si.l = append(si.l, fi)
			}
		}
	}
}

var (
	structInfoMutex  sync.RWMutex
	structInfoCache  = make(map[*reflect.StructType]*structInfo)
	defaultFieldInfo = &fieldInfo{}
)

func structInfoForType(t *reflect.StructType) *structInfo {

	structInfoMutex.RLock()
	si, found := structInfoCache[t]
	structInfoMutex.RUnlock()
	if found {
		return si
	}

	structInfoMutex.Lock()
	defer structInfoMutex.Unlock()
	si, found = structInfoCache[t]
	if found {
		return si
	}

	si = &structInfo{m: make(map[string]*fieldInfo)}
	compileStructInfo(t, make(map[string]int), nil, si)
	structInfoCache[t] = si
	return si
}
