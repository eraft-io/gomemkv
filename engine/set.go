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

type ISet interface {
	Add(item string)
	Size() int
	IsMember(key string) bool
	Members() []string
	RandMember() string
	Rem(item string)
}

type SkipListSet struct {
	sl *SkipList
}

func MakeSkipListSet() *SkipListSet {
	return &SkipListSet{
		sl: NewSkipList(),
	}
}

func (set *SkipListSet) Add(item string) {
	set.sl.Insert(item, nil)
}

func (set *SkipListSet) Size() int {
	return set.sl.Length()
}

func (set *SkipListSet) IsMember(key string) bool {
	ks, _ := set.sl.RangeQuery(key, key)
	return len(ks) > 0
}

func (set *SkipListSet) Members() []string {
	members := []string{}
	keys, _ := set.sl.IterAllNodes()
	for _, k := range keys {
		members = append(members, k.(string))
	}
	return members
}

func (set *SkipListSet) RandMember() string {
	ks, _ := set.sl.RandomQuery()
	return ks.(string)
}

func (set *SkipListSet) Rem(item string) {
	set.sl.Delete(item)
}
