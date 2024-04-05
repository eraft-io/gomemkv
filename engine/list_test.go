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
	"testing"
)

func TestDoublyLinkedListInt(t *testing.T) {
	list := MakeDoubleLinkedList()

	list.Append(1)
	list.Append(2)
	list.Append(3)

	list.Prepend(0)

	fmt.Print("Original List: ")
	list.Print()

	node := list.Find(2)
	if node != nil {
		fmt.Println("Node found:", node.data)
	} else {
		fmt.Println("Node not found")
	}

	list.Remove(2)

	fmt.Print("List after removing node 2: ")
	list.Print()

	list.InsertAfter(1, 2)

	fmt.Print("List after inserting node 2: ")
	list.Print()

	fmt.Printf("range query 2-3 %v", list.RangeQuery(2, 3))
}

func TestDoublyLinkedListString(t *testing.T) {

	list := MakeDoubleLinkedList()

	list.Append("b")
	list.Append("c")
	list.Append("d")

	list.Prepend("a")

	fmt.Print("Original List: ")
	list.Print()

	fmt.Printf("range query b-c %v", list.RangeQuery("b", "d"))

	node := list.Find("b")
	if node != nil {
		fmt.Println("Node found:", node.data)
	} else {
		fmt.Println("Node not found")
	}

	list.Remove("b")

	fmt.Print("List after removing node b: ")
	list.Print()

	list.InsertAfter("d", "ee")

	fmt.Print("List after inserting node d: ")
	list.Print()

	vals, _ := list.IndexRangeQuery(0, 1)
	fmt.Printf("List IndexRangeQuery: %v \n", vals)

	list.PopHead()
	fmt.Print("List after PopHead\n")

	list.Print()

	list.PopTail()
	fmt.Print("List after PopTail\n")

	list.Print()
}
