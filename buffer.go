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
	"encoding/binary"
)

var wire = binary.LittleEndian

// Buffer wraps a byte slice with convenience methods for writing BSON
// encodings and MongoDB messages.
type Buffer []byte

func (b *Buffer) Next(n int) []byte {
	begin := len(*b)
	end := begin + n
	if end > cap(*b) {
		noob := make([]byte, begin, 2*cap(*b)+n)
		copy(noob, *b)
		*b = noob
	}
	*b = (*b)[:end]
	return (*b)[begin:end]
}

func (b *Buffer) WriteString(s string) {
	copy(b.Next(len(s)), s)
}

func (b *Buffer) Write(p []byte) {
	copy(b.Next(len(p)), p)
}

func (b *Buffer) WriteByte(n byte) {
	b.Next(1)[0] = n
}

func (b *Buffer) WriteUint32(n uint32) {
	wire.PutUint32(b.Next(4), n)
}

func (b *Buffer) WriteUint64(n uint64) {
	wire.PutUint64(b.Next(8), n)
}