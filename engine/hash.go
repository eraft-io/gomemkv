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

type IHash interface {
	Insert(key string, val interface{})

	Delete(key string)

	Search(key string) interface{}
}

type SkipListHash struct {
	list *SkipList
}

func MakeSkipListHash() *SkipListHash {
	return &SkipListHash{
		list: NewSkipList(),
	}
}

func (sh *SkipListHash) Insert(key string, val interface{}) {
	sh.list.Insert(key, val)
}

func (sh *SkipListHash) Delete(key string) {
	sh.list.Delete(key)
}

func (sh *SkipListHash) Search(key string) interface{} {
	return sh.list.Search(key)
}

func (sh *SkipListHash) RandomQuery() (interface{}, interface{}) {
	return sh.list.RandomQuery()
}

func (sh *SkipListHash) IterAllNodes() ([]interface{}, []interface{}) {
	return sh.list.IterAllNodes()
}

func (sh *SkipListHash) Length() int {
	return sh.list.Length()
}
