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

import pb "github.com/eraft-io/gomemkv/raftpb"

type IRaftLog interface {
	GetFirstLogId() uint64
	GetLastLogId() uint64
	ResetFirstLogEntry(term int64, index int64) error
	ReInitLogs() error
	GetFirst() *pb.Entry
	GetLast() *pb.Entry
	LogItemCount() int
	Append(newEnt *pb.Entry)
	EraseBefore(logidx int64, withDel bool) ([]*pb.Entry, error)
	EraseAfter(logidx int64, withDel bool) []*pb.Entry
	GetRange(lo, hi int64) []*pb.Entry
	GetEntry(idx int64) *pb.Entry
	PersistRaftState(curTerm int64, votedFor int64, appliedId int64)
	ReadRaftState() (curTerm int64, votedFor int64, appliedId int64)
}
