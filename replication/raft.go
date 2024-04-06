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
	"context"
	"log"
	"sort"
	"sync"
	"time"

	pb "github.com/eraft-io/gomemkv/raftpb"
)

type NodeRole uint8

const (
	RoleFollower NodeRole = iota
	RoleCandidate
	RoleLeader
)

func RoleStr(role NodeRole) string {
	switch role {
	case RoleCandidate:
		return "Candidate"
	case RoleFollower:
		return "Follower"
	case RoleLeader:
		return "Leader"
	}
	return "unknow"
}

type Raft struct {
	mu               sync.RWMutex
	peers            []*PeerNode
	me               int
	applyCh          chan *pb.ApplyMsg
	applyCond        *sync.Cond
	replicationCond  []*sync.Cond
	role             NodeRole
	curTerm          int64
	votedFor         int64
	grantedVotes     int
	wals             *MemWal
	commitId         int64
	lastAppliedId    int64
	nextIds          []int
	matchIds         []int
	leaderId         int64
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer
	heartbeatTimeout uint64
	electionTimeout  uint64
}

func MakeRaft(peers []*PeerNode, me int, applyCh chan *pb.ApplyMsg, heartbeatTimeOutMs uint64, electionTimeOutMs uint64) *Raft {
	rf := &Raft{
		peers:            peers,
		me:               me,
		applyCh:          applyCh,
		replicationCond:  make([]*sync.Cond, len(peers)),
		role:             RoleFollower,
		curTerm:          0,
		votedFor:         -1,
		grantedVotes:     0,
		wals:             MakeMemWal(),
		nextIds:          make([]int, len(peers)),
		matchIds:         make([]int, len(peers)),
		heartbeatTimer:   time.NewTimer(time.Millisecond * time.Duration(heartbeatTimeOutMs)),
		electionTimer:    time.NewTimer(time.Millisecond * time.Duration(RandUInt64Range(electionTimeOutMs, 2*electionTimeOutMs))),
		heartbeatTimeout: heartbeatTimeOutMs,
		electionTimeout:  electionTimeOutMs,
	}

	rf.applyCond = sync.NewCond(&rf.mu)

	return rf
}

func (rf *Raft) RoleTransform(role NodeRole) {
	if rf.role == role {
		return
	}
	rf.role = role
	switch role {
	case RoleFollower:
		rf.heartbeatTimer.Stop()
		rf.ResetElectionTimeout()
	case RoleCandidate:
	case RoleLeader:
		lastWal := rf.wals.GetLastEntry()
		rf.leaderId = int64(rf.me)
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIds[i], rf.nextIds[i] = 0, int(lastWal.Index+1)
		}
		rf.electionTimer.Stop()
		rf.ResetHeartbeatTimeout()
	}
}

func (rf *Raft) IncrTerm() {
	rf.curTerm += 1
}

func (rf *Raft) IncrGrantedVotes() {
	rf.grantedVotes += 1
}

func (rf *Raft) ResetElectionTimeout() {
	rf.electionTimer.Reset(time.Millisecond * time.Duration(RandUInt64Range(rf.electionTimeout, 2*rf.electionTimeout)))
}

func (rf *Raft) ResetHeartbeatTimeout() {
	rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartbeatTimeout))
}

func (rf *Raft) HandleRequestVote(req *pb.RequestVoteRequest, resp *pb.RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.curTerm || (req.Term == rf.curTerm && rf.votedFor != -1 && rf.votedFor != req.CandidateId) {
		resp.Term, resp.VoteGranted = rf.curTerm, false
		return
	}

	if req.Term > rf.curTerm {
		rf.RoleTransform(RoleFollower)
		rf.curTerm, rf.votedFor = req.Term, -1
	}

	lastWal := rf.wals.GetLastEntry()

	if !(req.LastLogTerm > int64(lastWal.Term) || (req.LastLogTerm == int64(lastWal.Term) && req.LastLogIndex >= lastWal.Index)) {
		resp.Term, resp.VoteGranted = rf.curTerm, false
		return
	}

	rf.votedFor = req.CandidateId
	rf.ResetElectionTimeout()
	resp.Term, resp.VoteGranted = rf.curTerm, true
}

func (rf *Raft) LeaderId() int64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.leaderId
}

func (rf *Raft) advanceCommitIndexForLeader() {
	sort.Ints(rf.matchIds)
	n := len(rf.matchIds)
	// [18 18 '19 19 20] majority replicate log index 19
	// [18 '18 19] majority replicate log index 18
	// [18 '18 19 20] majority replicate log index 18
	new_commit_index := rf.matchIds[n-(n/2+1)]
	if new_commit_index > int(rf.commitId) {
		if rf.MatchLog(rf.curTerm, int64(new_commit_index)) {
			rf.commitId = int64(new_commit_index)
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	new_commit_index := Min(leaderCommit, int(rf.wals.GetLastEntry().Index))
	if new_commit_index > int(rf.commitId) {
		rf.commitId = int64(new_commit_index)
		rf.applyCond.Signal()
	}
}

// MatchLog is log matched
func (rf *Raft) MatchLog(term, index int64) bool {
	return index <= int64(rf.wals.GetLastEntry().Index) && rf.wals.GetEntry(index).Term == uint64(term)
}

func (rf *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest, resp *pb.AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.curTerm {
		resp.Term = rf.curTerm
		resp.Success = false
		return
	}

	if req.Term > rf.curTerm {
		rf.curTerm = req.Term
		rf.votedFor = -1
	}

	rf.RoleTransform(RoleFollower)
	rf.leaderId = req.LeaderId
	rf.ResetElectionTimeout()

	if req.PrevLogIndex < int64(rf.wals.GetFirstEntry().Index) {
		resp.Term = 0
		resp.Success = false
		return
	}

	if !rf.MatchLog(req.PrevLogTerm, req.PrevLogIndex) {
		resp.Term = rf.curTerm
		resp.Success = false
		last_index := rf.wals.GetLastEntry().Index
		if last_index < req.PrevLogIndex {
			resp.ConflictTerm = -1
			resp.ConflictIndex = last_index + 1
		} else {
			first_index := rf.wals.GetFirstEntry().Index
			resp.ConflictTerm = int64(rf.wals.GetEntry(req.PrevLogIndex).Term)
			index := req.PrevLogIndex - 1
			for index >= int64(first_index) && rf.wals.GetEntry(index).Term == uint64(resp.ConflictTerm) {
				index--
			}
			resp.ConflictIndex = index
		}
		return
	}

	first_index := rf.wals.GetFirstEntry().Index
	for index, entry := range req.Entries {
		if int(entry.Index-first_index) >= rf.wals.Count() || rf.wals.GetEntry(entry.Index).Term != entry.Term {
			rf.wals.EraseAfter(entry.Index - first_index)
			for _, newEnt := range req.Entries[index:] {
				rf.wals.Append(newEnt)
			}
			break
		}
	}

	rf.advanceCommitIndexForFollower(int(req.LeaderCommit))
	resp.Term = rf.curTerm
	resp.Success = true
}

// Election  make a new election
func (rf *Raft) Election() {

	rf.IncrGrantedVotes()
	rf.votedFor = int64(rf.me)
	vote_req := &pb.RequestVoteRequest{
		Term:         rf.curTerm,
		CandidateId:  int64(rf.me),
		LastLogIndex: int64(rf.wals.GetLastEntry().Index),
		LastLogTerm:  int64(rf.wals.GetLastEntry().Term),
	}
	for _, peer := range rf.peers {
		if int(peer.id) == rf.me {
			continue
		}
		go func(peer *PeerNode) {
			request_vote_resp, err := (*peer.rpcCli).RequestVote(context.Background(), vote_req)
			if err != nil {
				log.Default().Fatalf("send request vote to %s failed %v", peer.address, err.Error())
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if request_vote_resp != nil && rf.curTerm == vote_req.Term && rf.role == RoleCandidate {
				log.Default().Fatalf("send request vote to %s recive -> %s, curterm %d, req term %d", peer.address, request_vote_resp.String(), rf.curTerm, vote_req.Term)
				if request_vote_resp.VoteGranted {
					// success granted the votes
					rf.IncrGrantedVotes()
					if rf.grantedVotes > len(rf.peers)/2 {
						log.Default().Fatalf("I'm win this term, (node %d) get majority votes int term %d ", rf.me, rf.curTerm)
						rf.RoleTransform(RoleLeader)
						// rf.()
						rf.grantedVotes = 0
					}
				} else if request_vote_resp.Term > rf.curTerm {
					// request vote reject
					rf.RoleTransform(RoleFollower)
					rf.curTerm, rf.votedFor = request_vote_resp.Term, -1
				}
			}
		}(peer)
	}
}

//
// BroadcastAppend broadcast append to peers
//

func (rf *Raft) BroadcastAppend() {
	for _, peer := range rf.peers {
		if peer.id == uint64(rf.me) {
			continue
		}
		rf.replicationCond[peer.id].Signal()
	}
}

// BroadcastHeartbeat broadcast heartbeat to peers
func (rf *Raft) BroadcastHeartbeat() {
	for _, peer := range rf.peers {
		if int(peer.id) == rf.me {
			continue
		}
		log.Default().Printf("send heart beat to %s", peer.address)
		go func(peer *PeerNode) {
			rf.replicateOneRound(peer)
		}(peer)
	}
}

// Tick raft heart, this ticket trigger raft main flow running
func (rf *Raft) Tick() {
	for {
		select {
		case <-rf.electionTimer.C:
			{
				rf.RoleTransform(RoleCandidate)
				rf.IncrTerm()
				rf.Election()
				rf.ResetElectionTimeout()
			}
		case <-rf.heartbeatTimer.C:
			{
				if rf.role == RoleLeader {
					rf.BroadcastHeartbeat()
					rf.ResetHeartbeatTimeout()
				}
			}
		}
	}
}

//
// Propose the interface to the appplication propose a operation
//

func (rf *Raft) Propose(payload []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		return -1, -1, false
	}
	// if rf.isSnapshoting {
	// 	return -1, -1, false
	// }
	newLog := rf.Append(payload)
	rf.BroadcastAppend()
	return int(newLog.Index), int(newLog.Term), true
}

//
// Append append a new command to it's logs
//

func (rf *Raft) Append(command []byte) *pb.Entry {
	lastWal := rf.wals.GetLastEntry()
	newLog := &pb.Entry{
		Index: lastWal.Index + 1,
		Term:  uint64(rf.curTerm),
		Data:  command,
	}
	rf.wals.Append(newLog)
	rf.matchIds[rf.me] = int(newLog.Index)
	rf.nextIds[rf.me] = int(newLog.Index) + 1
	return newLog
}

// Replicator manager duplicate run
func (rf *Raft) Replicator(peer *PeerNode) {
	rf.replicationCond[peer.id].L.Lock()
	defer rf.replicationCond[peer.id].L.Unlock()
	for {
		log.Default().Printf("peer id wait for replicating...")
		for !(rf.role == RoleLeader && rf.matchIds[peer.id] < int(rf.wals.GetLastEntry().Index)) {
			rf.replicationCond[peer.id].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

// replicateOneRound duplicate log entries to other nodes in the cluster
func (rf *Raft) replicateOneRound(peer *PeerNode) {
	rf.mu.RLock()
	if rf.role != RoleLeader {
		rf.mu.RUnlock()
		return
	}
	prev_log_index := uint64(rf.nextIds[peer.id] - 1)
	log.Default().Printf("leader prev log index %d", prev_log_index)
	if prev_log_index < uint64(rf.wals.GetFirstEntry().Index) {

	} else {
		first_index := rf.wals.GetFirstEntry().Index
		log.Default().Printf("first log index %d", first_index)
		new_ents := rf.wals.EraseBefore(int64(prev_log_index) + 1)
		entries := make([]*pb.Entry, len(new_ents))
		copy(entries, new_ents)

		append_ent_req := &pb.AppendEntriesRequest{
			Term:         rf.curTerm,
			LeaderId:     int64(rf.me),
			PrevLogIndex: int64(prev_log_index),
			PrevLogTerm:  int64(rf.wals.GetEntry(int64(prev_log_index)).Term),
			Entries:      entries,
			LeaderCommit: rf.commitId,
		}
		rf.mu.RUnlock()

		// send empty ae to peers
		resp, err := (*peer.rpcCli).AppendEntries(context.Background(), append_ent_req)
		if err != nil {
			log.Default().Fatalf("send append entries to %s failed %v\n", peer.address, err.Error())
		}
		if rf.role == RoleLeader && rf.curTerm == append_ent_req.Term {
			if resp != nil {
				// deal with appendRnt resp
				if resp.Success {
					log.Default().Printf("send heart beat to %s success", peer.address)
					rf.matchIds[peer.id] = int(append_ent_req.PrevLogIndex) + len(append_ent_req.Entries)
					rf.nextIds[peer.id] = rf.matchIds[peer.id] + 1
					rf.advanceCommitIndexForLeader()
				} else {
					// there is a new leader in group
					if resp.Term > rf.curTerm {
						rf.RoleTransform(RoleFollower)
						rf.curTerm = resp.Term
						rf.votedFor = -1
					} else if resp.Term == rf.curTerm {
						rf.nextIds[peer.id] = int(resp.ConflictIndex)
						if resp.ConflictTerm != -1 {
							for i := append_ent_req.PrevLogIndex; i >= int64(first_index); i-- {
								if rf.wals.GetEntry(i).Term == uint64(resp.ConflictTerm) {
									rf.nextIds[peer.id] = int(i + 1)
									break
								}
							}
						}
					}
				}
			}
		}
	}
}

// Applier() Write the commited message to the applyCh channel
// and update lastApplied
func (rf *Raft) Applier() {
	for {
		rf.mu.Lock()
		for rf.lastAppliedId >= rf.commitId {
			log.Default().Println("applier ...")
			rf.applyCond.Wait()
		}

		commit_index, last_applied := rf.commitId, rf.lastAppliedId
		entries := make([]*pb.Entry, commit_index-last_applied)
		copy(entries, rf.wals.GetRange(last_applied+1, commit_index))
		log.Default().Printf("%d, applies entries %d-%d in term %d", rf.me, rf.lastAppliedId, commit_index, rf.curTerm)

		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- &pb.ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandTerm:  int64(entry.Term),
				CommandIndex: int64(entry.Index),
			}
		}

		rf.mu.Lock()
		rf.lastAppliedId = int64(Max(int(rf.lastAppliedId), int(commit_index)))
		rf.mu.Unlock()
	}
}
