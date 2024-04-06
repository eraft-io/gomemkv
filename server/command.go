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
	Exec(*MemkvClient, []interface{}) error
}

type SetCommand struct {
}

func (set *SetCommand) Exec(c *MemkvClient, params []interface{}) error {
	c.db.Insert(params[1].(string), engine.MakeGoStringFromByteSlice([]byte(params[2].(string))))
	_, err := c.conn.Write([]byte("+OK\r\n"))
	return err
}

type GetCommand struct {
}

func (get *GetCommand) Exec(c *MemkvClient, params []interface{}) error {
	val := c.db.Search(params[1].(string))
	if val == nil {
		_, err := c.conn.Write([]byte("$-1\r\n"))
		return err
	}
	valStr := val.(*engine.GoString).ToString()
	_, err := c.conn.Write([]byte(fmt.Sprintf("+%s\r\n", valStr)))
	return err
}

type AppendCommand struct{}

func (append *AppendCommand) Exec(c *MemkvClient, params []interface{}) error {
	val := c.db.Search(params[1].(string))
	if val == nil {
		newVal := engine.MakeGoStringFromByteSlice([]byte(params[2].(string)))
		c.db.Insert(params[1].(string), newVal)
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", newVal.Len())))
		return err
	}
	newVal := val.(*engine.GoString).AppendString(params[2].(string))
	c.db.Insert(params[1].(string), newVal)
	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", newVal.Len())))
	return err
}

type StrLenCommand struct {
}

func (strlen *StrLenCommand) Exec(c *MemkvClient, params []interface{}) error {
	val := c.db.Search(params[1].(string))
	if val == nil {
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		return err
	}
	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", val.(*engine.GoString).Len())))
	return err
}

type DelCommand struct {
}

func (del *DelCommand) Exec(c *MemkvClient, params []interface{}) error {
	c.db.Delete(params[1].(string))
	_, err := c.conn.Write([]byte("+OK\r\n"))
	return err
}

type SetRangeCommand struct {
}

func (setrange *SetRangeCommand) Exec(c *MemkvClient, params []interface{}) error {
	val := c.db.Search(params[1].(string))
	offset, err := strconv.Atoi(params[2].(string))
	if err != nil {
		return err
	}
	if val == nil {
		newVal := engine.MakeGoStringFromByteSlice([]byte{})
		for i := 0; i < offset; i++ {
			newVal.AppendString(" ")
		}
		newVal.AppendString(params[3].(string))
		log.Default().Println(newVal.ToString())
		c.db.Insert(params[1].(string), newVal)
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", newVal.Len())))
		return err
	}
	val.(*engine.GoString).SetRange(int64(offset), params[3].(string))
	_, err_ := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", val.(*engine.GoString).Len())))
	return err_
}

type UnknownCommand struct {
}

func (s *UnknownCommand) Exec(c *MemkvClient, params []interface{}) error {
	_, err := c.conn.Write([]byte(fmt.Sprintf("-ERR unknown command %s\r\n", params[0].(string))))
	return err
}

type LPushCommand struct {
}

func (lpush *LPushCommand) Exec(c *MemkvClient, params []interface{}) error {

	oldList := c.db.Search(params[1].(string))
	if oldList == nil {
		oldList = engine.MakeDoubleLinkedList()
	}

	for i := 2; i < len(params); i++ {
		oldList.(*engine.DoublyLinkedList).Prepend(params[i])
	}

	c.db.Insert(params[1].(string), oldList)

	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())))
	return err
}

type LPopCommand struct {
}

func (lpop *LPopCommand) Exec(c *MemkvClient, params []interface{}) error {
	oldList := c.db.Search(params[1].(string))
	if oldList == nil {
		_, err := c.conn.Write([]byte("$-1\r\n"))
		return err
	}

	head, err := oldList.(*engine.DoublyLinkedList).PopHead()
	if err != nil {
		return err
	}

	c.db.Insert(params[1].(string), oldList)

	_, err = c.conn.Write([]byte(fmt.Sprintf("+%s\r\n", head.(string))))
	return err
}

type LLenCommand struct {
}

func (llen *LLenCommand) Exec(c *MemkvClient, params []interface{}) error {
	oldList := c.db.Search(params[1].(string))
	if oldList == nil {
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		return err
	}
	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())))
	return err
}

type RPushCommand struct {
}

func (rpush *RPushCommand) Exec(c *MemkvClient, params []interface{}) error {
	oldList := c.db.Search(params[1].(string))
	if oldList == nil {
		oldList = engine.MakeDoubleLinkedList()
	}

	for i := 2; i < len(params); i++ {
		oldList.(*engine.DoublyLinkedList).Append(params[i])
	}

	c.db.Insert(params[1].(string), oldList)

	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", oldList.(*engine.DoublyLinkedList).Len())))
	return err
}

type RPopCommand struct {
}

func (rpop *RPopCommand) Exec(c *MemkvClient, params []interface{}) error {
	oldList := c.db.Search(params[1].(string))
	if oldList == nil {
		_, err := c.conn.Write([]byte("$-1\r\n"))
		return err
	}

	tail, err := oldList.(*engine.DoublyLinkedList).PopTail()
	if err != nil {
		return err
	}

	c.db.Insert(params[1].(string), oldList)

	_, err = c.conn.Write([]byte(fmt.Sprintf("+%s\r\n", tail.(string))))
	return err
}

type LRangeCommand struct {
}

func (lrange *LRangeCommand) Exec(c *MemkvClient, params []interface{}) error {

	key := params[1].(string)

	startIdx, err := strconv.Atoi(params[2].(string))
	if err != nil {
		return err
	}

	endIdx, err := strconv.Atoi(params[3].(string))
	if err != nil {
		return err
	}

	list := c.db.Search(key)
	if list == nil {
		_, err := c.conn.Write([]byte("*0\r\n"))
		return err
	}

	entrys, err := list.(*engine.DoublyLinkedList).IndexRangeQuery(startIdx, endIdx)
	if err != nil {
		return err
	}

	replyBuf := "*" + strconv.Itoa(len(entrys)) + "\r\n"
	for _, entry := range entrys {
		replyBuf += "$"
		replyBuf += strconv.Itoa(len(entry.(string)))
		replyBuf += "\r\n"
		replyBuf += entry.(string)
		replyBuf += "\r\n"
	}

	_, err = c.conn.Write([]byte(replyBuf))

	return err
}

type SAddCommand struct {
}

func (sadd *SAddCommand) Exec(c *MemkvClient, params []interface{}) error {
	key := params[1].(string)
	val := c.db.Search(key)
	if val != nil {
		oldSet := val.(*engine.SkipListSet)
		if oldSet.IsMember(params[2].(string)) {
			_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
			return err
		}
		oldSet.Add(params[2].(string))
		c.db.Insert(key, oldSet)
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 1)))
		return err
	} else {
		setVal := engine.MakeSkipListSet()
		setVal.Add(params[2].(string))
		c.db.Insert(key, setVal)
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 1)))
		return err
	}
}

type SMembersCommand struct {
}

func (smem *SMembersCommand) Exec(c *MemkvClient, params []interface{}) error {
	key := params[1].(string)
	set := c.db.Search(key)
	if set == nil {
		_, err := c.conn.Write([]byte("*0\r\n"))
		return err
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

	_, err := c.conn.Write([]byte(replyBuf))
	return err
}

type SCardCommand struct{}

func (scard *SCardCommand) Exec(c *MemkvClient, params []interface{}) error {
	key := params[1].(string)
	set := c.db.Search(key)
	if set == nil {
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		return err
	}
	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", set.(*engine.SkipListSet).Size())))
	return err
}

type SRandMemberCommand struct{}

func (srandMember *SRandMemberCommand) Exec(c *MemkvClient, params []interface{}) error {
	key := params[1].(string)
	set := c.db.Search(key)
	if set == nil {
		_, err := c.conn.Write([]byte("$-1\r\n"))
		return err
	}
	randMember := set.(*engine.SkipListSet).RandMember()
	_, err := c.conn.Write([]byte(fmt.Sprintf("+%s\r\n", randMember)))
	return err
}

type SRemCommand struct{}

func (srem *SRemCommand) Exec(c *MemkvClient, params []interface{}) error {
	key := params[1].(string)
	set := c.db.Search(key)
	if set == nil {
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		return err
	}
	if !set.(*engine.SkipListSet).IsMember(params[2].(string)) {
		_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		return err
	}
	set.(*engine.SkipListSet).Rem(params[2].(string))
	_, err := c.conn.Write([]byte(fmt.Sprintf(":%d\r\n", 1)))
	return err
}
