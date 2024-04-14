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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/eraft-io/gomemkv/logger"
	pb "github.com/eraft-io/gomemkv/raftpb"
	storage_eng "github.com/eraft-io/gomemkv/storage"
)

type PersisRaftLog struct {
	mu       sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	dbEng    storage_eng.KvStore
}

type RaftPersistenState struct {
	CurTerm   int64
	VotedFor  int64
	AppliedId int64
}

// MakePersistRaftLog make a persist raft log model
//
// newdbEng: a LevelDBKvStore storage engine
func MakePersistRaftLog(newdbEng storage_eng.KvStore) *PersisRaftLog {
	_, err := newdbEng.GetBytesValue(EncodeRaftLogKey(INIT_LOG_INDEX))
	if err != nil {
		logger.ELogger().Sugar().Debugf("init raft log state")
		emp_ent := &pb.Entry{}
		emp_ent_encode := EncodeEntry(emp_ent)
		newdbEng.PutBytesKv(EncodeRaftLogKey(INIT_LOG_INDEX), emp_ent_encode)
		return &PersisRaftLog{dbEng: newdbEng}
	}
	lidkBytes, _, err := newdbEng.SeekPrefixLast(RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	lastIdx := binary.BigEndian.Uint64(lidkBytes[len(RAFTLOG_PREFIX):])
	fidkBytes, _, err := newdbEng.SeekPrefixFirst(RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	firstIdx := binary.BigEndian.Uint64(fidkBytes[len(RAFTLOG_PREFIX):])
	return &PersisRaftLog{dbEng: newdbEng, lastIdx: lastIdx, firstIdx: firstIdx}
}

// PersistRaftState Persistent storage raft state
// (curTerm, and votedFor)
// you can find this design in raft paper figure2 State definition
func (rfLog *PersisRaftLog) PersistRaftState(curTerm int64, votedFor int64, appliedId int64) {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	rf_state := &RaftPersistenState{
		CurTerm:   curTerm,
		VotedFor:  votedFor,
		AppliedId: appliedId,
	}
	rfLog.dbEng.PutBytesKv(RAFT_STATE_KEY, EncodeRaftState(rf_state))
}

// ReadRaftState
// read the persist curTerm, votedFor for node from storage engine
func (rfLog *PersisRaftLog) ReadRaftState() (curTerm int64, votedFor int64, appliedId int64) {
	rfBytes, err := rfLog.dbEng.GetBytesValue(RAFT_STATE_KEY)
	if err != nil {
		return 0, -1, 0
	}
	rfState := DecodeRaftState(rfBytes)
	return rfState.CurTerm, rfState.VotedFor, rfState.AppliedId
}

// GetFirstLogId
// get the first log id from storage engine
func (rfLog *PersisRaftLog) GetFirstLogId() uint64 {
	return rfLog.firstIdx
}

// GetLastLogId
//
// get the last log id from storage engine
func (rfLog *PersisRaftLog) GetLastLogId() uint64 {
	return rfLog.lastIdx
}

func (rfLog *PersisRaftLog) ResetFirstLogEntry(term int64, index int64) error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	newEnt := &pb.Entry{}
	newEnt.EntryType = pb.EntryType_EntryNormal
	newEnt.Term = uint64(term)
	newEnt.Index = index
	newEntEncode := EncodeEntry(newEnt)
	if err := rfLog.dbEng.PutBytesKv(EncodeRaftLogKey(uint64(index)), newEntEncode); err != nil {
		return err
	}
	rfLog.firstIdx = uint64(index)
	return nil
}

// ReInitLogs
// make logs to init state
func (rfLog *PersisRaftLog) ReInitLogs() error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	// delete all log
	if err := rfLog.dbEng.DelPrefixKeys(RAFTLOG_PREFIX); err != nil {
		return err
	}
	rfLog.firstIdx = 0
	rfLog.lastIdx = 1
	// add a empty
	empEnt := &pb.Entry{}
	empentEncode := EncodeEntry(empEnt)
	return rfLog.dbEng.PutBytesKv(EncodeRaftLogKey(INIT_LOG_INDEX), empentEncode)
}

// GetFirst
//
// get the first entry from storage engine
func (rfLog *PersisRaftLog) GetFirst() *pb.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return rfLog.getEnt(int64(rfLog.firstIdx))
}

// GetLast
//
// get the last entry from storage engine
func (rfLog *PersisRaftLog) GetLast() *pb.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return rfLog.getEnt(int64(rfLog.lastIdx))
}

// LogItemCount
//
// get total log count from storage engine
func (rfLog *PersisRaftLog) LogItemCount() int {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return int(rfLog.lastIdx) - int(rfLog.firstIdx) + 1
}

// Append
//
// append a new entry to raftlog, put it to storage engine
func (rfLog *PersisRaftLog) Append(newEnt *pb.Entry) {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	newentEncode := EncodeEntry(newEnt)
	rfLog.dbEng.PutBytesKv(EncodeRaftLogKey(uint64(rfLog.lastIdx)+1), newentEncode)
	rfLog.lastIdx += 1
}

// EraseBefore
// erase log before from idx, and copy [idx:] log return
// this operation don't modity log in storage engine
func (rfLog *PersisRaftLog) EraseBefore(logidx int64, withDel bool) ([]*pb.Entry, error) {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	ents := []*pb.Entry{}
	lastlogId := rfLog.GetLastLogId()
	firstlogId := rfLog.GetFirstLogId()
	if withDel {
		for i := firstlogId; i < uint64(logidx); i++ {
			if err := rfLog.dbEng.DeleteBytesK(EncodeRaftLogKey(i)); err != nil {
				return ents, err
			}
			logger.ELogger().Sugar().Debugf("del log with id %d success", i)
		}
		rfLog.firstIdx = uint64(logidx)
	}
	for i := logidx; i <= int64(lastlogId); i++ {
		ents = append(ents, rfLog.getEnt(i))
	}
	return ents, nil
}

// EraseAfter
// erase after idx, !!!WRANNING!!! is withDel is true, this operation will delete log key
// in storage engine
func (rfLog *PersisRaftLog) EraseAfter(logidx int64, withDel bool) []*pb.Entry {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	firstlogId := rfLog.GetFirstLogId()
	if withDel {
		for i := logidx; i <= int64(rfLog.GetLastLogId()); i++ {
			if err := rfLog.dbEng.DeleteBytesK(EncodeRaftLogKey(uint64(i))); err != nil {
				panic(err)
			}
		}
		rfLog.lastIdx = uint64(logidx) - 1
	}
	ents := []*pb.Entry{}
	for i := firstlogId; i < uint64(logidx); i++ {
		ents = append(ents, rfLog.getEnt(int64(i)))
	}
	return ents
}

// GetRange
// get range log from storage engine, and return the copy
// [lo, hi)
func (rfLog *PersisRaftLog) GetRange(lo, hi int64) []*pb.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	ents := []*pb.Entry{}
	for i := lo; i <= hi; i++ {
		ents = append(ents, rfLog.getEnt(i))
	}
	return ents
}

// GetEntry
// get log entry with idx
func (rfLog *PersisRaftLog) GetEntry(idx int64) *pb.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return rfLog.getEnt(idx)
}

func (rfLog *PersisRaftLog) getEnt(logidx int64) *pb.Entry {
	encodeValue, err := rfLog.dbEng.GetBytesValue(EncodeRaftLogKey(uint64(logidx)))
	if err != nil {
		logger.ELogger().Sugar().Debugf("get log entry with id %d error!", logidx)
		panic(err)
	}
	return DecodeEntry(encodeValue)
}

// EncodeRaftLogKey
// encode raft log key with perfix -> RAFTLOG_PREFIX
func EncodeRaftLogKey(idx uint64) []byte {
	var out_buf bytes.Buffer
	out_buf.Write(RAFTLOG_PREFIX)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(idx))
	out_buf.Write(b)
	return out_buf.Bytes()
}

// DecodeRaftLogKey
// deocde raft log key, return log id
func DecodeRaftLogKey(bts []byte) uint64 {
	return binary.BigEndian.Uint64(bts[4:])
}

// EncodeEntry
// encode log entry to bytes sequence
func EncodeEntry(ent *pb.Entry) []byte {
	var bytesEnt bytes.Buffer
	enc := gob.NewEncoder(&bytesEnt)
	enc.Encode(ent)
	return bytesEnt.Bytes()
}

// DecodeEntry
// decode log entry from bytes sequence
func DecodeEntry(in []byte) *pb.Entry {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	ent := pb.Entry{}
	dec.Decode(&ent)
	return &ent
}

// EncodeRaftState
// encode RaftPersistenState to bytes sequence
func EncodeRaftState(rfState *RaftPersistenState) []byte {
	var bytesState bytes.Buffer
	enc := gob.NewEncoder(&bytesState)
	enc.Encode(rfState)
	return bytesState.Bytes()
}

// DecodeRaftState
// decode RaftPersistenState from bytes sequence
func DecodeRaftState(in []byte) *RaftPersistenState {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	rfState := RaftPersistenState{}
	dec.Decode(&rfState)
	return &rfState
}
