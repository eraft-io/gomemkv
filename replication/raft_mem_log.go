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

package replication

import (
	"sync"

	pb "github.com/eraft-io/gomemkv/raftpb"
)

type MemLog struct {
	mu      sync.RWMutex
	firstId uint64
	lastId  uint64
	entries map[uint64]*pb.Entry
}

func MakeMemLog() *MemLog {
	// add an empty entry
	entries := make(map[uint64]*pb.Entry, 0)
	ent := &pb.Entry{}
	entries[INIT_LOG_INDEX] = ent
	return &MemLog{entries: entries, firstId: INIT_LOG_INDEX, lastId: INIT_LOG_INDEX}
}

func (memLog *MemLog) GetFirstLogId() uint64 {
	return memLog.firstId
}

func (memLog *MemLog) GetLastLogId() uint64 {
	return memLog.lastId
}

func (memLog *MemLog) ReInitLogs() error {
	memLog.mu.Lock()
	defer memLog.mu.Unlock()
	memLog.firstId = INIT_LOG_INDEX
	memLog.lastId = INIT_LOG_INDEX
	empEnt := &pb.Entry{}
	memLog.entries[INIT_LOG_INDEX] = empEnt
	return nil
}

func (memLog *MemLog) ResetFirstLogEntry(term int64, index int64) error {
	return nil
}

func (memLog *MemLog) PersistRaftState(curTerm int64, votedFor int64, appliedId int64) {
}

func (memLog *MemLog) ReadRaftState() (curTerm int64, votedFor int64, appliedId int64) {
	return 0, 0, 0
}

func (memLog *MemLog) GetFirst() *pb.Entry {
	memLog.mu.RLock()
	defer memLog.mu.RUnlock()
	return memLog.entries[memLog.firstId]
}

func (memLog *MemLog) GetLast() *pb.Entry {
	memLog.mu.RLock()
	defer memLog.mu.RUnlock()
	return memLog.entries[memLog.lastId]
}

func (memLog *MemLog) LogItemCount() int {
	memLog.mu.RLock()
	defer memLog.mu.RUnlock()
	return int(memLog.lastId) - int(memLog.firstId) + 1
}

func (memLog *MemLog) Append(newEnt *pb.Entry) {
	memLog.mu.Lock()
	defer memLog.mu.Unlock()
	memLog.entries[memLog.lastId+1] = newEnt
	memLog.lastId += 1
}

func (memLog *MemLog) EraseBefore(logidx int64, withDel bool) ([]*pb.Entry, error) {
	memLog.mu.Lock()
	defer memLog.mu.Unlock()
	ents := []*pb.Entry{}
	lastlogId := memLog.GetLastLogId()
	firstlogId := memLog.GetFirstLogId()
	if withDel {
		for i := firstlogId; i < uint64(logidx); i++ {
			delete(memLog.entries, i)
		}
		memLog.firstId = uint64(logidx)
	}
	for i := logidx; i <= int64(lastlogId); i++ {
		ents = append(ents, memLog.getEntry(i))
	}
	return ents, nil
}

func (memLog *MemLog) EraseAfter(logidx int64, withDel bool) []*pb.Entry {
	memLog.mu.Lock()
	defer memLog.mu.Unlock()
	firstlogId := memLog.GetFirstLogId()
	if withDel {
		for i := logidx; i <= int64(memLog.GetLastLogId()); i++ {
			delete(memLog.entries, uint64(i))
		}
		memLog.lastId = uint64(logidx) - 1
	}
	ents := []*pb.Entry{}
	for i := firstlogId; i < uint64(logidx); i++ {
		ents = append(ents, memLog.getEntry(int64(i)))
	}
	return ents
}

func (memLog *MemLog) GetRange(lo, hi int64) []*pb.Entry {
	memLog.mu.RLock()
	defer memLog.mu.RUnlock()
	ents := []*pb.Entry{}
	for i := lo; i <= hi; i++ {
		ents = append(ents, memLog.getEntry(i))
	}
	return ents
}

func (memLog *MemLog) GetEntry(idx int64) *pb.Entry {
	memLog.mu.RLock()
	defer memLog.mu.RUnlock()
	return memLog.getEntry(idx)
}

func (memLog *MemLog) getEntry(idx int64) *pb.Entry {
	return memLog.entries[uint64(idx)]
}
