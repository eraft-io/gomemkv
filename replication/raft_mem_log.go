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
	pb "github.com/eraft-io/gomemkv/raftpb"
)

type MemLog struct {
	firstId uint64
	lastId  uint64
	entries []*pb.Entry
}

func MakeMemLog() *MemLog {
	// add an empty entry
	entries := []*pb.Entry{}
	ent := &pb.Entry{}
	entries = append(entries, ent)
	return &MemLog{entries: entries, firstId: 0, lastId: 1}
}

func (memWal *MemLog) GetFirstEntry() *pb.Entry {
	return memWal.entries[0]
}

func (memWal *MemLog) GetLastEntry() *pb.Entry {
	return memWal.entries[len(memWal.entries)-1]
}

func (memVal *MemLog) Count() int {
	return len(memVal.entries)
}

func (memVal *MemLog) EraseBefore(id int64) []*pb.Entry {
	return memVal.entries[id:]
}

func (memVal *MemLog) EraseAfter(id int64) []*pb.Entry {
	return memVal.entries[:id]
}

func (memVal *MemLog) GetRange(start, end int64) []*pb.Entry {
	return memVal.entries[start : end+1]
}

func (memVal *MemLog) Append(ent *pb.Entry) {
	memVal.entries = append(memVal.entries, ent)
}

func (memVal *MemLog) GetEntry(id int64) *pb.Entry {
	return memVal.entries[id]
}
