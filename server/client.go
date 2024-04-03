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
	"log"
	"net"
	"time"
)

type MemkvClient struct {
	conn       *net.TCPConn
	qbuf       []byte
	createTime time.Time
}

func MakeMemkvClient(c *net.TCPConn) *MemkvClient {
	return &MemkvClient{
		conn:       c,
		createTime: time.Now(),
		qbuf:       make([]byte, 128),
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

		log.Default().Printf("qbuf %v", c.qbuf)

		c.conn.Write(c.qbuf)
	}

}
