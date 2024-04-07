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

package storage

// If you want to contribute a new engine implementation, you need to implement these interfaces
type KvStore interface {
	Put(string, string) error
	Get(string) (string, error)
	Delete(string) error
	DumpPrefixKey(string) (map[string]string, error)
	PutBytesKv([]byte, []byte) error
	DeleteBytesK([]byte) error
	GetBytesValue([]byte) ([]byte, error)
	SeekPrefixLast([]byte) ([]byte, []byte, error)
	SeekPrefixFirst([]byte) ([]byte, []byte, error)
	DelPrefixKeys([]byte) error
	FlushDB()
}

func EngineFactory(name string, dbPath string) KvStore {
	switch name {
	case "leveldb":
		levelDb, err := MakeLevelDBKvStore(dbPath)
		if err != nil {
			panic(err)
		}
		return levelDb
	default:
		panic("no such engine type support")
	}
}
