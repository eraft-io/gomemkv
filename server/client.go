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
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
)

type MemkvClient struct {
	conn       *net.TCPConn
	qbuf       []byte
	createTime time.Time
	db         *map[string]string
}

func MakeMemkvClient(c *net.TCPConn, s *MemKvServer) *MemkvClient {
	return &MemkvClient{
		conn:       c,
		createTime: time.Now(),
		qbuf:       make([]byte, 1024),
		db:         s.db,
	}
}

func (c *MemkvClient) ProccessRequest() {

	for {
		n, err := c.conn.Read(c.qbuf)
		if err != nil {
			log.Default().Printf("read from client err: %v", err.Error())
			return
		}
		if n == 0 {
			log.Default().Printf("client closed connection")
			return
		}

		parser := NewRESParser(bytes.NewReader(c.qbuf))

		res, _ := parser.Parser()

		switch res.(type) {
		// client Arrays request
		case []interface{}:
			// log.Default().Printf("qbuf %s parser res %v type %s", string(c.qbuf), res, reflect.TypeOf(res))
			arr := res.([]interface{})
			if arr[0].(string) == "set" {
				(*c.db)[arr[1].(string)] = arr[2].(string)
				c.conn.Write([]byte("+OK\r\n"))
			} else if arr[0].(string) == "get" {
				val := (*c.db)[arr[1].(string)]
				c.conn.Write([]byte(fmt.Sprintf("+%s\r\n", val)))
			} else {
				c.conn.Write([]byte("+OK\r\n"))
			}
		default:
			c.conn.Write([]byte("-ERR\r\n"))
		}

	}

}
