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

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDBKvStore struct {
	Path string
	db   *leveldb.DB
}

func MakeLevelDBKvStore(path string) (*LevelDBKvStore, error) {
	newDB, err := leveldb.OpenFile(path, &opt.Options{})
	if err != nil {
		return nil, err
	}
	return &LevelDBKvStore{
		Path: path,
		db:   newDB,
	}, nil
}

func (levelDB *LevelDBKvStore) PutBytesKv(k []byte, v []byte) error {
	return levelDB.db.Put(k, v, nil)
}

func (levelDB *LevelDBKvStore) DeleteBytesK(k []byte) error {
	return levelDB.db.Delete(k, nil)
}

func (levelDB *LevelDBKvStore) GetBytesValue(k []byte) ([]byte, error) {
	return levelDB.db.Get(k, nil)
}

func (levelDB *LevelDBKvStore) Put(k string, v string) error {
	return levelDB.db.Put([]byte(k), []byte(v), nil)
}

func (levelDB *LevelDBKvStore) Get(k string) (string, error) {
	v, err := levelDB.db.Get([]byte(k), nil)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (levelDB *LevelDBKvStore) Delete(k string) error {
	return levelDB.db.Delete([]byte(k), nil)
}

func (levelDB *LevelDBKvStore) DumpPrefixKey(prefix string) (map[string]string, error) {
	kvs := make(map[string]string)
	iter := levelDB.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		k := string(iter.Key())
		v := string(iter.Value())
		kvs[k] = v
	}
	iter.Release()
	return kvs, iter.Error()
}

func (levelDB *LevelDBKvStore) FlushDB() {

}

func (levelDB *LevelDBKvStore) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	iter := levelDB.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	ok := iter.Last()
	var keyBytes, valBytes []byte
	if ok {
		keyBytes = iter.Key()
		valBytes = iter.Value()
	}
	return keyBytes, valBytes, nil
}

func (levelDB *LevelDBKvStore) SeekPrefixFirst(prefix []byte) ([]byte, []byte, error) {
	iter := levelDB.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	if iter.Next() {
		return iter.Key(), iter.Value(), nil
	}
	return []byte{}, []byte{}, errors.New("seek not find key")
}

func (levelDB *LevelDBKvStore) DelPrefixKeys(prefix []byte) error {
	iter := levelDB.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		err := levelDB.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return nil
}
