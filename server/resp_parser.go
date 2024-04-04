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

package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type RESPParser struct {
	reader *bufio.Reader
}

func NewRESParser(reader io.Reader) *RESPParser {
	return &RESPParser{
		reader: bufio.NewReader(reader),
	}
}

func (p *RESPParser) Parser() (interface{}, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 3 {
		return nil, fmt.Errorf("invalid RESP message")
	}

	switch line[0] {
	// Simple strings
	case '+':
		return string(line[1 : len(line)-2]), nil
	// Simple Errors
	case '-':
		return nil, fmt.Errorf("error: %s", line[1:len(line)-2])
	// Integers
	case ':':
		num, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse integer: %w", err)
		}
		return num, nil
	// Bulk strings
	case '$':
		size, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return nil, fmt.Errorf("failed to parse bulk string size: %w", err)
		}
		if size < 0 {
			return nil, nil
		}
		data := make([]byte, size)
		_, err = io.ReadFull(p.reader, data)
		if err != nil {
			return nil, fmt.Errorf("failed to read bulk string data: %w", err)
		}
		// Discard trailing CRLF
		p.reader.Discard(2)
		return string(data), nil
	// Arrays
	case '*':
		size, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return nil, fmt.Errorf("failed to parse array size: %w", err)
		}
		array := make([]interface{}, size)
		for i := range array {
			item, err := p.Parser()
			if err != nil {
				return nil, fmt.Errorf("failed to read array item: %w", err)
			}
			array[i] = item
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unknown RESP message type")
	}

}
