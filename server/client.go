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
	"strings"
	"time"

	"github.com/eraft-io/gomemkv/engine"
)

type MemkvClient struct {
	conn       *net.TCPConn
	qbuf       []byte
	createTime time.Time
	db         engine.IHash
}

func MakeMemkvClient(c *net.TCPConn, s *MemKvServer) *MemkvClient {
	return &MemkvClient{
		conn:       c,
		createTime: time.Now(),
		qbuf:       make([]byte, 1024),
		db:         s.db,
	}
}

func (c *MemkvClient) ExecCmd(params []interface{}) {
	switch strings.ToLower(params[0].(string)) {
	case "set":
		set := SetCommand{}
		set.Exec(c, params)
	case "get":
		get := GetCommand{}
		get.Exec(c, params)
	case "del":
		del := DelCommand{}
		del.Exec(c, params)
	case "append":
		append := AppendCommand{}
		append.Exec(c, params)
	case "strlen":
		strlen := StrLenCommand{}
		strlen.Exec(c, params)
	case "setrange":
		setrange := SetRangeCommand{}
		setrange.Exec(c, params)
	case "lpush":
		lpush := LPushCommand{}
		lpush.Exec(c, params)
	case "lpop":
		lpop := LPopCommand{}
		lpop.Exec(c, params)
	case "rpush":
		rpush := RPushCommand{}
		rpush.Exec(c, params)
	case "rpop":
		rpop := RPopCommand{}
		rpop.Exec(c, params)
	case "lrange":
		lrange := LRangeCommand{}
		lrange.Exec(c, params)
	case "llen":
		llen := LLenCommand{}
		llen.Exec(c, params)
	case "sadd":
		sadd := SAddCommand{}
		sadd.Exec(c, params)
	case "smembers":
		smembers := SMembersCommand{}
		smembers.Exec(c, params)
	case "scard":
		scard := SCardCommand{}
		scard.Exec(c, params)
	case "srandmember":
		srandmember := SRandMemberCommand{}
		srandmember.Exec(c, params)
	case "srem":
		srem := SRemCommand{}
		srem.Exec(c, params)
	default:
		unknow := UnknownCommand{}
		unknow.Exec(c, params)
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

		switch v := res.(type) {
		// client Arrays request
		case []interface{}:
			params := res.([]interface{})
			c.ExecCmd(params)
		default:
			c.conn.Write([]byte(fmt.Sprintf("-ERR protocol paser result type %s\r\n", v)))
		}

	}

}
