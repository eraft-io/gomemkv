// Copyright [2024] [jayjieliu]

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"strings"
)

// Abstraction of Golang String objects
type IGoString interface {
	Len() int
	Clear()
	AppendString(s string) *GoString
	AppendGoString(gs *GoString) *GoString
	Range(lo int64, hi int64) *GoString
	SetRange(offset int64, s string)
	ToLower() *GoString
	ToUpper() *GoString
	ToString() string
}

type GoString struct {
	buf []byte
}

func MakeGoString() *GoString {
	return &GoString{
		buf: make([]byte, 0),
	}
}

func MakeGoStringFromByteSlice(nbuf []byte) *GoString {
	return &GoString{
		buf: nbuf,
	}
}

func (s *GoString) Len() int {
	return len(s.buf)
}

func (s *GoString) Clear() {
	s.buf = s.buf[:0]
}

func (s *GoString) AppendString(ns string) *GoString {
	s.buf = append(s.buf, []byte(ns)...)
	return s
}

func (s *GoString) AppendGoString(gs *GoString) *GoString {
	s.buf = append(s.buf, gs.buf...)
	return s
}

func (s *GoString) Range(lo int64, hi int64) *GoString {
	newBuf := s.buf[lo:hi]
	return MakeGoStringFromByteSlice(newBuf)
}

func (s *GoString) SetRange(offset int64, ss string) {
	newBuf := s.buf[:offset]
	newBuf = append(newBuf, []byte(ss)...)
	s.buf = newBuf
}

func (s *GoString) ToLower() *GoString {
	lowerStr := strings.ToLower(s.ToString())
	return MakeGoStringFromByteSlice([]byte(lowerStr))
}

func (s *GoString) ToUpper() *GoString {
	upperStr := strings.ToUpper(s.ToString())
	return MakeGoStringFromByteSlice([]byte(upperStr))
}

func (s *GoString) ToString() string {
	return string(s.buf)
}

func (s *GoString) Buf() []byte {
	return s.buf
}
