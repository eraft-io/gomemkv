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
