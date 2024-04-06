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
	"fmt"
	"math/rand"
	"time"
)

const MaxLevel = 32

type SkipNode struct {
	key     string
	value   interface{}
	forward []*SkipNode
}

type SkipList struct {
	level  int
	header *SkipNode
	length int
	rand   *rand.Rand
}

func NewSkipNode(key string, value interface{}, level int) *SkipNode {
	return &SkipNode{
		key:     key,
		value:   value,
		forward: make([]*SkipNode, level),
	}
}

// Create a new skiplist
func NewSkipList() *SkipList {
	return &SkipList{
		level:  1,
		header: NewSkipNode("", nil, MaxLevel),
		rand:   rand.New(rand.NewSource(42)),
	}
}

func (list *SkipList) randomLevel() int {
	level := 1
	for level < MaxLevel && list.rand.Float64() < 0.5 {
		level++
	}
	return level
}

func (list *SkipList) Insert(key string, value interface{}) {
	update := make([]*SkipNode, MaxLevel)
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x == nil || x.key != key {
		level := list.randomLevel()
		if level > list.level {
			for i := list.level; i < level; i++ {
				update[i] = list.header
			}
			list.level = level
		}
		x = NewSkipNode(key, value, level)
		for i := 0; i < level; i++ {
			x.forward[i] = update[i].forward[i]
			update[i].forward[i] = x
		}
		list.length++
	} else {
		x.value = value
	}
}

func (list *SkipList) Delete(key string) {
	update := make([]*SkipNode, MaxLevel)
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		for i := 0; i < list.level; i++ {
			if update[i].forward[i] != x {
				break
			}
			update[i].forward[i] = x.forward[i]
		}
		for list.level > 1 && list.header.forward[list.level-1] == nil {
			list.level--
		}
		list.length--
	}
}

func (list *SkipList) Search(key string) interface{} {
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		return x.value
	}
	return nil
}

func (list *SkipList) Length() int {
	return list.length
}

func (list *SkipList) RangeQuery(start, end string) ([]string, []interface{}) {
	resultKeys := []string{}
	resultVals := []interface{}{}

	current := list.header
	for i := list.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < start {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	for current != nil && current.key <= end {
		resultKeys = append(resultKeys, current.key)
		resultVals = append(resultVals, current.value)
		current = current.forward[0]
	}

	return resultKeys, resultVals
}

func (list *SkipList) IterAllNodes() ([]interface{}, []interface{}) {
	resultKeys := []interface{}{}
	resultVals := []interface{}{}
	x := list.header.forward[0]
	for x != nil {
		// fmt.Printf("%s ", x.key)
		resultKeys = append(resultKeys, x.key)
		resultVals = append(resultVals, x.value)
		x = x.forward[0]
	}
	return resultKeys, resultVals
}

func (list *SkipList) RandomQuery() (interface{}, interface{}) {
	current := list.header
	list.rand = rand.New(rand.NewSource(time.Now().Unix()))
	level := rand.Intn(list.level)
	for i := level; i >= 0; i-- {
		if current.forward[i] != nil {
			current = current.forward[i]
		}
	}
	return current.key, current.value
}

func (list *SkipList) Print() {
	fmt.Printf("SkipList, level: %d, length: %d\n", list.level, list.length)
	for i := list.level - 1; i >= 0; i-- {
		fmt.Printf("Level[%d]: ", i)
		x := list.header.forward[i]
		for x != nil {
			fmt.Printf("%s ", x.key)
			x = x.forward[i]
		}
		fmt.Println()
	}
}
