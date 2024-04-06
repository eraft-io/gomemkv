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

type MemWal struct {
	firstId   uint64
	lastId    uint64
	appliedId uint64
	entries   []*pb.Entry
}

func MakeMemWal() *MemWal {
	// add an empty entry
	ent := &pb.Entry{}
	entries := []*pb.Entry{}
	entries = append(entries, ent)
	return &MemWal{entries: entries, firstId: 0, lastId: 1}
}

func (memWal *MemWal) GetFirstEntry() *pb.Entry {
	return memWal.entries[0]
}

func (memWal *MemWal) GetLastEntry() *pb.Entry {
	return memWal.entries[len(memWal.entries)-1]
}

func (memVal *MemWal) Count() int {
	return len(memVal.entries)
}

func (memVal *MemWal) EraseBefore(id int64) []*pb.Entry {
	return memVal.entries[id:]
}

func (memVal *MemWal) EraseAfter(id int64) []*pb.Entry {
	return memVal.entries[:id]
}

func (memVal *MemWal) GetRange(start, end int64) []*pb.Entry {
	return memVal.entries[start:end]
}

func (memVal *MemWal) Append(ent *pb.Entry) {
	memVal.entries = append(memVal.entries, ent)
}

func (memVal *MemWal) GetEntry(id int64) *pb.Entry {
	return memVal.entries[id]
}
