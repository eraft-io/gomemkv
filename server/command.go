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
	"fmt"
	"log"
	"strconv"

	"github.com/eraft-io/gomemkv/engine"
)

type ICommand interface {
	Exec(*MemkvClient, []string) ([]byte, error)
}

type SetCommand struct {
}

func (set *SetCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	c.db.Insert(params[1], engine.MakeGoStringFromByteSlice([]byte(params[2])))
	if c.svr.rf.IsLeader() {
		return []byte("+OK\r\n"), nil
	}
	return []byte{}, nil
}

type GetCommand struct {
}

func (get *GetCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	val := c.db.Search(params[1])
	if val == nil {
		return []byte("$-1\r\n"), nil
	}
	valStr := val.(*engine.GoString).ToString()
	return []byte(fmt.Sprintf("+%s\r\n", valStr)), nil
}

type AppendCommand struct{}

func (append *AppendCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	val := c.db.Search(params[1])
	if val == nil {
		newVal := engine.MakeGoStringFromByteSlice([]byte(params[2]))
		c.db.Insert(params[1], newVal)
		return []byte(fmt.Sprintf(":%d\r\n", newVal.Len())), nil
	}
	newVal := val.(*engine.GoString).AppendString(params[2])
	c.db.Insert(params[1], newVal)
	return []byte(fmt.Sprintf(":%d\r\n", newVal.Len())), nil
}

type StrLenCommand struct {
}

func (strlen *StrLenCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	val := c.db.Search(params[1])
	if val == nil {
		return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
	}
	return []byte(fmt.Sprintf(":%d\r\n", val.(*engine.GoString).Len())), nil
}

type DelCommand struct {
}

func (del *DelCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	c.db.Delete(params[1])
	return []byte("+OK\r\n"), nil
}

type SetRangeCommand struct {
}

func (setrange *SetRangeCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	val := c.db.Search(params[1])
	offset, err := strconv.Atoi(params[2])
	if err != nil {
		return []byte{}, err
	}
	if val == nil {
		newVal := engine.MakeGoStringFromByteSlice([]byte{})
		for i := 0; i < offset; i++ {
			newVal.AppendString(" ")
		}
		newVal.AppendString(params[3])
		log.Default().Println(newVal.ToString())
		c.db.Insert(params[1], newVal)
		return []byte(fmt.Sprintf(":%d\r\n", newVal.Len())), nil
	}
	val.(*engine.GoString).SetRange(int64(offset), params[3])
	return []byte(fmt.Sprintf(":%d\r\n", val.(*engine.GoString).Len())), nil
}

type UnknownCommand struct {
}

func (s *UnknownCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	return []byte(fmt.Sprintf("-ERR unknown command %s\r\n", params[0])), nil
}

type LPushCommand struct {
}

func (lpush *LPushCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {

	oldList := c.db.Search(params[1])
	if oldList == nil {
		oldList = engine.MakeDoubleLinkedList()
	}

	for i := 2; i < len(params); i++ {
		oldList.(*engine.DoublyLinkedList).Prepend(params[i])
	}

	c.db.Insert(params[1], oldList)

	return []byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())), nil
}

type LPopCommand struct {
}

func (lpop *LPopCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	oldList := c.db.Search(params[1])
	if oldList == nil {
		return []byte("$-1\r\n"), nil
	}

	head, err := oldList.(*engine.DoublyLinkedList).PopHead()
	if err != nil {
		return []byte{}, err
	}

	c.db.Insert(params[1], oldList)

	return []byte(fmt.Sprintf("+%s\r\n", head.(string))), nil
}

type LLenCommand struct {
}

func (llen *LLenCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	oldList := c.db.Search(params[1])
	if oldList == nil {
		return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
	}
	return []byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())), nil
}

type RPushCommand struct {
}

func (rpush *RPushCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	oldList := c.db.Search(params[1])
	if oldList == nil {
		oldList = engine.MakeDoubleLinkedList()
	}

	for i := 2; i < len(params); i++ {
		oldList.(*engine.DoublyLinkedList).Append(params[i])
	}

	c.db.Insert(params[1], oldList)

	return []byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())), nil
}

type RPopCommand struct {
}

func (rpop *RPopCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	oldList := c.db.Search(params[1])
	if oldList == nil {
		return []byte("$-1\r\n"), nil
	}

	tail, err := oldList.(*engine.DoublyLinkedList).PopTail()
	if err != nil {
		return []byte{}, err
	}

	c.db.Insert(params[1], oldList)

	return []byte(fmt.Sprintf("+%s\r\n", tail.(string))), nil
}

type LRangeCommand struct {
}

func (lrange *LRangeCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {

	key := params[1]

	startIdx, err := strconv.Atoi(params[2])
	if err != nil {
		return []byte{}, err
	}

	endIdx, err := strconv.Atoi(params[3])
	if err != nil {
		return []byte{}, err
	}

	list := c.db.Search(key)
	if list == nil {
		_, err := c.conn.Write([]byte("*0\r\n"))
		return []byte{}, err
	}

	entrys, err := list.(*engine.DoublyLinkedList).IndexRangeQuery(startIdx, endIdx)
	if err != nil {
		return []byte{}, err
	}

	replyBuf := "*" + strconv.Itoa(len(entrys)) + "\r\n"
	for _, entry := range entrys {
		replyBuf += "$"
		replyBuf += strconv.Itoa(len(entry.(string)))
		replyBuf += "\r\n"
		replyBuf += entry.(string)
		replyBuf += "\r\n"
	}

	return []byte(replyBuf), nil
}

type SAddCommand struct {
}

func (sadd *SAddCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]
	val := c.db.Search(key)
	if val != nil {
		oldSet := val.(*engine.SkipListSet)
		if oldSet.IsMember(params[2]) {
			return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
		}
		oldSet.Add(params[2])
		c.db.Insert(key, oldSet)
		return []byte(fmt.Sprintf(":%d\r\n", 1)), nil
	} else {
		setVal := engine.MakeSkipListSet()
		setVal.Add(params[2])
		c.db.Insert(key, setVal)
		return []byte(fmt.Sprintf(":%d\r\n", 1)), nil
	}
}

type SMembersCommand struct {
}

func (smem *SMembersCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]
	set := c.db.Search(key)
	if set == nil {
		return []byte("*0\r\n"), nil
	}
	members := set.(*engine.SkipListSet).Members()
	replyBuf := "*" + strconv.Itoa(len(members)) + "\r\n"
	for _, m := range members {
		replyBuf += "$"
		replyBuf += strconv.Itoa(len(m))
		replyBuf += "\r\n"
		replyBuf += m
		replyBuf += "\r\n"
	}

	return []byte(replyBuf), nil
}

type SCardCommand struct{}

func (scard *SCardCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]
	set := c.db.Search(key)
	if set == nil {
		return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
	}
	return []byte(fmt.Sprintf(":%d\r\n", set.(*engine.SkipListSet).Size())), nil
}

type SRandMemberCommand struct{}

func (srandMember *SRandMemberCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]
	set := c.db.Search(key)
	if set == nil {
		return []byte("$-1\r\n"), nil
	}
	randMember := set.(*engine.SkipListSet).RandMember()
	return []byte(fmt.Sprintf("+%s\r\n", randMember)), nil
}

type SRemCommand struct{}

func (srem *SRemCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]
	set := c.db.Search(key)
	if set == nil {
		return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
	}
	if !set.(*engine.SkipListSet).IsMember(params[2]) {
		return []byte(fmt.Sprintf(":%d\r\n", 0)), nil
	}
	set.(*engine.SkipListSet).Rem(params[2])
	return []byte(fmt.Sprintf(":%d\r\n", 1)), nil
}

type HSetCommand struct{}

func (hset *HSetCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]

	hash := engine.MakeSkipListHash()

	count := 0
	for i := 2; i < len(params); i += 2 {
		hash.Insert(params[i], params[i+1])
		count++
	}

	c.db.Insert(key, hash)

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

type HGetCommand struct{}

func (hget *HGetCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]

	hashVal := c.db.Search(key)

	val := hashVal.(*engine.SkipListHash).Search(params[2])

	if val == nil {
		return []byte("$-1\r\n"), nil
	}

	return []byte(fmt.Sprintf("+%s\r\n", val.(string))), nil
}

type HGetAllCommand struct{}

func (hgetall *HGetAllCommand) Exec(c *MemkvClient, params []string) ([]byte, error) {
	key := params[1]

	hashVal := c.db.Search(key)
	keys, vals := hashVal.(*engine.SkipListHash).IterAllNodes()
	replyBuf := "*" + strconv.Itoa(len(keys)*2) + "\r\n"

	for i := 0; i < len(keys); i++ {
		replyBuf += "$"
		replyBuf += strconv.Itoa(len(keys[i].(string)))
		replyBuf += "\r\n"
		replyBuf += keys[i].(string)
		replyBuf += "\r\n"
		replyBuf += "$"
		replyBuf += strconv.Itoa(len(vals[i].(string)))
		replyBuf += "\r\n"
		replyBuf += vals[i].(string)
		replyBuf += "\r\n"
	}

	return []byte(replyBuf), nil
}
