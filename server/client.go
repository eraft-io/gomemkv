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
	"encoding/json"
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
	svr        *MemKvServer
	id         int
}

func MakeMemkvClient(c *net.TCPConn, s *MemKvServer, id int) *MemkvClient {
	return &MemkvClient{
		conn:       c,
		createTime: time.Now(),
		qbuf:       make([]byte, 1024),
		db:         s.db,
		svr:        s,
		id:         id,
	}
}

func (c *MemkvClient) ExecCmd(params []string) ([]byte, error) {
	switch strings.ToLower(params[0]) {
	case "set":
		set := SetCommand{}
		return set.Exec(c, params)
	case "get":
		get := GetCommand{}
		return get.Exec(c, params)
	case "del":
		del := DelCommand{}
		return del.Exec(c, params)
	case "append":
		append := AppendCommand{}
		return append.Exec(c, params)
	case "strlen":
		strlen := StrLenCommand{}
		return strlen.Exec(c, params)
	case "setrange":
		setrange := SetRangeCommand{}
		return setrange.Exec(c, params)
	case "lpush":
		lpush := LPushCommand{}
		return lpush.Exec(c, params)
	case "lpop":
		lpop := LPopCommand{}
		return lpop.Exec(c, params)
	case "rpush":
		rpush := RPushCommand{}
		return rpush.Exec(c, params)
	case "rpop":
		rpop := RPopCommand{}
		return rpop.Exec(c, params)
	case "lrange":
		lrange := LRangeCommand{}
		return lrange.Exec(c, params)
	case "llen":
		llen := LLenCommand{}
		return llen.Exec(c, params)
	case "sadd":
		sadd := SAddCommand{}
		return sadd.Exec(c, params)
	case "smembers":
		smembers := SMembersCommand{}
		return smembers.Exec(c, params)
	case "scard":
		scard := SCardCommand{}
		return scard.Exec(c, params)
	case "srandmember":
		srandmember := SRandMemberCommand{}
		return srandmember.Exec(c, params)
	case "srem":
		srem := SRemCommand{}
		return srem.Exec(c, params)
	case "hset":
		hset := HSetCommand{}
		return hset.Exec(c, params)
	case "hget":
		hget := HGetCommand{}
		return hget.Exec(c, params)
	case "hgetall":
		hgetall := HGetAllCommand{}
		return hgetall.Exec(c, params)
	default:
		unknow := UnknownCommand{}
		return unknow.Exec(c, params)
	}
}

type CmdParams struct {
	Params []string
}

func (c *MemkvClient) ProccessRequest() {

	for {
		n, err := c.conn.Read(c.qbuf)
		if err != nil {
			log.Default().Printf("client %d closed connection with err: %v", c.id, err.Error())
			c.svr.clis[c.id] = nil
			return
		}
		if n == 0 {
			c.svr.clis[c.id] = nil
			log.Default().Printf("client closed connection")
			return
		}

		if strings.Contains(string(c.qbuf), "COMMAND") {
			log.Default().Printf("init cmd")
			c.conn.Write([]byte("+OK\r\n"))
			continue
		}

		parser := NewRESParser(bytes.NewReader(c.qbuf))

		res, _ := parser.Parser()

		cmdParams := &CmdParams{}

		switch v := res.(type) {
		// client Arrays request
		case []interface{}:
			params := res.([]interface{})
			for _, pm := range params {
				cmdParams.Params = append(cmdParams.Params, pm.(string))
			}
		default:
			c.conn.Write([]byte(fmt.Sprintf("-ERR protocol paser result type %s\r\n", v)))
		}

		in, _ := json.Marshal(cmdParams)

		// propose to raft to replicate
		id, _, isLeader := c.svr.rf.Propose(in, int64(c.id))
		if !isLeader {
			c.conn.Write([]byte("-ERR not leader \r\n"))
			continue
		}

		c.svr.mu.Lock()
		ch := c.svr.GetNotifyChan(id)
		c.svr.mu.Unlock()

		select {
		case _ = <-ch:
			// log.Default().Printf("%v", res)
		case <-time.After(time.Second * 5):
			c.conn.Write([]byte("-ERR exec cmd timeout \r\n"))
		}

		go func() {
			c.svr.mu.Lock()
			delete(c.svr.notifyChans, id)
			c.svr.mu.Unlock()
		}()

	}

}
