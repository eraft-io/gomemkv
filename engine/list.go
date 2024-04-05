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
	"errors"
	"fmt"
	"reflect"
)

type Node struct {
	data interface{}
	next *Node
	prev *Node
}

type DoublyLinkedList struct {
	head   *Node
	tail   *Node
	length int
}

func MakeDoubleLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{
		length: 0,
	}
}

func (list *DoublyLinkedList) Append(newdata interface{}) {
	newNode := &Node{data: newdata}

	if list.tail == nil {
		list.head = newNode
		list.tail = newNode
		list.length++
		return
	}

	lastNode := list.tail
	lastNode.next = newNode
	newNode.prev = lastNode
	list.tail = newNode
	list.length++
}

func (list *DoublyLinkedList) Prepend(data interface{}) {

	newNode := &Node{data: data}

	if list.head == nil {
		list.head = newNode
		list.tail = newNode
		list.length++
		return
	}

	firstNode := list.head
	newNode.next = firstNode
	firstNode.prev = newNode
	list.head = newNode
	list.length++
}

func (list *DoublyLinkedList) PopHead() (interface{}, error) {
	if list.head == nil {
		return nil, errors.New("empty list")
	}

	firstNode := list.head
	list.head = firstNode.next
	if list.head != nil {
		list.head.prev = nil
	} else {
		list.tail = nil
	}
	list.length--
	return firstNode.data, nil
}

func (list *DoublyLinkedList) PopTail() (interface{}, error) {
	if list.tail == nil {
		return nil, errors.New("empty list")
	}

	lastNode := list.tail
	list.tail = lastNode.prev
	if list.tail != nil {
		list.tail.next = nil
	} else {
		list.head = nil
	}
	list.length--
	return lastNode.data, nil
}

func (list *DoublyLinkedList) Find(data interface{}) *Node {
	current := list.head
	for current != nil {
		if current.data == data {
			return current
		}
		current = current.next
	}
	return nil
}

func (list *DoublyLinkedList) Remove(data interface{}) {
	node := list.Find(data)
	if node == nil {
		return
	}

	if node == list.head {
		list.head = node.next
		if list.head != nil {
			list.head.prev = nil
		}
	} else if node == list.tail {
		list.tail = node.prev
		if list.tail != nil {
			list.tail.next = nil
		}
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}
	list.length--
}

func (list *DoublyLinkedList) InsertAfter(data, newData interface{}) {
	node := list.Find(data)
	if node == nil {
		return
	}

	newNode := &Node{data: newData, prev: node, next: node.next}
	node.next = newNode
	if newNode.next != nil {
		newNode.next.prev = newNode
	}
	if node == list.tail {
		list.tail = newNode
	}
	list.length++
}

func (list *DoublyLinkedList) Len() int {
	return list.length
}

func compareValues(val1, val2 interface{}) (int, error) {
	if reflect.TypeOf(val1) != reflect.TypeOf(val2) {
		return 0, fmt.Errorf("values are not of the same type")
	}

	v1 := reflect.ValueOf(val1)
	v2 := reflect.ValueOf(val2)

	if !v1.Type().Comparable() || !v2.Type().Comparable() {
		return 0, fmt.Errorf("values are not comparable")
	}

	switch v1.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v1.Int() > v2.Int() {
			return 1, nil
		} else if v1.Int() < v2.Int() {
			return -1, nil
		} else {
			return 0, nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v1.Uint() > v2.Uint() {
			return 1, nil
		} else if v1.Uint() < v2.Uint() {
			return -1, nil
		} else {
			return 0, nil
		}
	case reflect.Float32, reflect.Float64:
		if v1.Float() > v2.Float() {
			return 1, nil
		} else if v1.Float() < v2.Float() {
			return -1, nil
		} else {
			return 0, nil
		}
	case reflect.String:
		if v1.String() > v2.String() {
			return 1, nil
		} else if v1.String() < v2.String() {
			return -1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("unsupported value type")
	}
}

func (list *DoublyLinkedList) RangeQuery(start, end interface{}) []interface{} {
	var result []interface{}
	current := list.head
	for current != nil {
		biggerThenStart, _ := compareValues(current.data, start)
		lessThenEnd, _ := compareValues(current.data, end)
		if biggerThenStart >= 0 && lessThenEnd <= 0 {
			result = append(result, current.data)
		}
		current = current.next
	}
	return result
}

func (list *DoublyLinkedList) IndexRangeQuery(startIndex, endIndex int) ([]interface{}, error) {
	if startIndex < 0 {
		return nil, errors.New("invalid index range")
	}

	var result []interface{}
	current := list.head
	index := 0
	for current != nil && (endIndex < 0 || index <= endIndex) {
		if index >= startIndex {
			result = append(result, current.data)
		}
		current = current.next
		index++
	}
	return result, nil
}

func (list *DoublyLinkedList) Print() {
	current := list.head
	for current != nil {
		fmt.Print(current.data, " ")
		current = current.next
	}
	fmt.Println()
}
