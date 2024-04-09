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
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eraft-io/gomemkv/logger"
	pb "github.com/eraft-io/gomemkv/raftpb"
	storage_eng "github.com/eraft-io/gomemkv/storage"
)

type NodeRole uint8

// raft node state
const (
	NodeRoleFollower NodeRole = iota
	NodeRoleCandidate
	NodeRoleLeader
)

func NodeToString(role NodeRole) string {
	switch role {
	case NodeRoleCandidate:
		return "Candidate"
	case NodeRoleFollower:
		return "Follower"
	case NodeRoleLeader:
		return "Leader"
	}
	return "unknow"
}

// raft stack definition
type Raft struct {
	mu               sync.RWMutex
	peers            []*RaftPeerNode // rpc client end
	id               int64
	dead             int32
	applyCh          chan *pb.ApplyMsg
	applyCond        *sync.Cond
	replicatorCond   []*sync.Cond
	role             NodeRole
	curTerm          int64
	votedFor         int64
	grantedVotes     int
	logs             *RaftLog
	commitIdx        int64
	lastApplied      int64
	nextIdx          []int
	matchIdx         []int
	isSnapshoting    bool
	leaderId         int64
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer
	heartBeatTimeout uint64
	baseElecTimeout  uint64
}

func MakeRaft(peers []*RaftPeerNode, me int64, newdbEng storage_eng.KvStore, applyCh chan *pb.ApplyMsg, heartbeatTimeOutMs uint64, baseElectionTimeOutMs uint64) *Raft {
	rf := &Raft{
		peers:            peers,
		id:               me,
		dead:             0,
		applyCh:          applyCh,
		replicatorCond:   make([]*sync.Cond, len(peers)),
		role:             NodeRoleFollower,
		curTerm:          0,
		votedFor:         VOTE_FOR_NO_ONE,
		grantedVotes:     0,
		isSnapshoting:    false,
		logs:             MakePersistRaftLog(newdbEng),
		nextIdx:          make([]int, len(peers)),
		matchIdx:         make([]int, len(peers)),
		heartbeatTimer:   time.NewTimer(time.Millisecond * time.Duration(heartbeatTimeOutMs)),
		electionTimer:    time.NewTimer(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(baseElectionTimeOutMs)))),
		baseElecTimeout:  baseElectionTimeOutMs,
		heartBeatTimeout: heartbeatTimeOutMs,
	}
	rf.curTerm, rf.votedFor = rf.logs.ReadRaftState()
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.logs.GetLast()
	for _, peer := range peers {
		rf.matchIdx[peer.id], rf.nextIdx[peer.id] = 0, int(lastLog.Index+1)
		if int64(peer.id) != me {
			rf.replicatorCond[peer.id] = sync.NewCond(&sync.Mutex{})
			go rf.Replicator(peer)
		}
	}

	go rf.Tick()

	go rf.Applier()

	return rf
}

func (rf *Raft) PersistRaftState() {
	rf.logs.PersistRaftState(rf.curTerm, rf.votedFor)
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) IsKilled() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) IsLeader() bool {
	return rf.leaderId == rf.id
}

func (rf *Raft) SwitchRaftNodeRole(role NodeRole) {
	if rf.role == role {
		return
	}
	rf.role = role
	logger.ELogger().Sugar().Debugf("node change role to -> %s \n", NodeToString(role))
	switch role {
	case NodeRoleFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElecTimeout))) * time.Millisecond)
	case NodeRoleCandidate:
	case NodeRoleLeader:
		// become leader，set replica (matchIdx and nextIdx) processs table
		lastLog := rf.logs.GetLast()
		rf.leaderId = int64(rf.id)
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIdx[i], rf.nextIdx[i] = 0, int(lastLog.Index+1)
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(time.Duration(rf.heartBeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) IncrCurrentTerm() {
	rf.curTerm += 1
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return int(rf.curTerm), rf.role == NodeRoleLeader
}

func (rf *Raft) IncrGrantedVotes() {
	rf.grantedVotes += 1
}

// HandleRequestVote  handle request vote from other node
func (rf *Raft) HandleRequestVote(req *pb.RequestVoteRequest, resp *pb.RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.PersistRaftState()

	if req.Term < rf.curTerm || (req.Term == rf.curTerm && rf.votedFor != -1 && rf.votedFor != req.CandidateId) {
		resp.Term, resp.VoteGranted = rf.curTerm, false
		return
	}

	if req.Term > rf.curTerm {
		rf.SwitchRaftNodeRole(NodeRoleFollower)
		rf.curTerm, rf.votedFor = req.Term, -1
	}

	lastLog := rf.logs.GetLast()

	if req.LastLogTerm < int64(lastLog.Term) || (req.LastLogTerm == int64(lastLog.Term) && req.LastLogIndex < lastLog.Index) {
		resp.Term, resp.VoteGranted = rf.curTerm, false
		return
	}

	rf.votedFor = req.CandidateId
	rf.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElecTimeout))))
	resp.Term, resp.VoteGranted = rf.curTerm, true
}

//
// HandleRequestVote  handle append entries from other node
//

func (rf *Raft) GetLeaderId() int64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.leaderId
}

func (rf *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest, resp *pb.AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.PersistRaftState()

	if req.Term < rf.curTerm {
		resp.Term = rf.curTerm
		resp.Success = false
		return
	}

	if req.Term > rf.curTerm {
		rf.curTerm = req.Term
		rf.votedFor = VOTE_FOR_NO_ONE
	}

	rf.SwitchRaftNodeRole(NodeRoleFollower)
	rf.leaderId = req.LeaderId
	rf.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElecTimeout))))

	if req.PrevLogIndex < int64(rf.logs.GetFirst().Index) {
		resp.Term = 0
		resp.Success = false
		logger.ELogger().Sugar().Debugf("peer %d reject append entires request from %d", rf.id, req.LeaderId)
		return
	}

	if !rf.MatchLog(req.PrevLogTerm, req.PrevLogIndex) {
		resp.Term = rf.curTerm
		resp.Success = false
		lastIndex := rf.logs.GetLast().Index
		if lastIndex < req.PrevLogIndex {
			logger.ELogger().Sugar().Warnf("log confict with term %d, index %d", -1, lastIndex+1)
			resp.ConflictTerm = -1
			resp.ConflictIndex = lastIndex + 1
		} else {
			firstIndex := rf.logs.GetFirst().Index
			resp.ConflictTerm = int64(rf.logs.GetEntry(req.PrevLogIndex).Term)
			index := req.PrevLogIndex - 1
			for index >= int64(firstIndex) && rf.logs.GetEntry(index).Term == uint64(resp.ConflictTerm) {
				index--
			}
			resp.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.logs.GetFirst().Index
	for index, entry := range req.Entries {
		if int(entry.Index-firstIndex) >= rf.logs.LogItemCount() || rf.logs.GetEntry(entry.Index).Term != entry.Term {
			rf.logs.EraseAfter(entry.Index-firstIndex, true)
			for _, newEnt := range req.Entries[index:] {
				rf.logs.Append(newEnt)
			}
			break
		}
	}

	rf.advanceCommitIndexForFollower(int(req.LeaderCommit))
	resp.Term = rf.curTerm
	resp.Success = true
}

func (rf *Raft) CondInstallSnapshot(lastIncluedTerm int, lastIncludedIndex int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= int(rf.commitIdx) {
		return false
	}

	if lastIncludedIndex > int(rf.logs.GetLast().Index) {
		rf.logs.ReInitLogs()
	} else {
		rf.logs.EraseBefore(int64(lastIncludedIndex), true)
	}
	rf.logs.ResetFirstLogEntry(int64(lastIncluedTerm), int64(lastIncludedIndex))

	rf.lastApplied = int64(lastIncludedIndex)
	rf.commitIdx = int64(lastIncludedIndex)

	return true
}

// install snapshot from leader
func (rf *Raft) HandleInstallSnapshot(request *pb.InstallSnapshotRequest, response *pb.InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	response.Term = rf.curTerm

	if request.Term < rf.curTerm {
		return
	}

	if request.Term > rf.curTerm {
		rf.curTerm = request.Term
		rf.votedFor = -1
		rf.PersistRaftState()
	}

	rf.SwitchRaftNodeRole(NodeRoleFollower)
	rf.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElecTimeout))))

	if request.LastIncludedIndex <= rf.commitIdx {
		return
	}

	go func() {
		rf.applyCh <- &pb.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()

}

func (rf *Raft) advanceCommitIndexForLeader() {
	sort.Ints(rf.matchIdx)
	n := len(rf.matchIdx)
	// [18 18 '19 19 20] majority replicate log index 19
	// [18 '18 19] majority replicate log index 18
	// [18 '18 19 20] majority replicate log index 18
	newCommitIndex := rf.matchIdx[n-(n/2+1)]
	if newCommitIndex > int(rf.commitIdx) {
		if rf.MatchLog(rf.curTerm, int64(newCommitIndex)) {
			logger.ELogger().Sugar().Debugf("leader advance commit lid %d index %d at term %d appliedId %d", rf.id, rf.commitIdx, rf.curTerm, rf.lastApplied)
			rf.commitIdx = int64(newCommitIndex)
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, int(rf.logs.GetLast().Index))
	if newCommitIndex > int(rf.commitIdx) {
		logger.ELogger().Sugar().Debugf("peer %d advance commit index %d at term %d appliedId %d", rf.id, rf.commitIdx, rf.curTerm, rf.lastApplied)
		rf.commitIdx = int64(newCommitIndex)
		rf.applyCond.Signal()
	}
}

// MatchLog is log matched
func (rf *Raft) MatchLog(term, index int64) bool {
	return index <= int64(rf.logs.GetLast().Index) && rf.logs.GetEntry(index).Term == uint64(term)
}

// Election  make a new election
func (rf *Raft) Election() {
	logger.ELogger().Sugar().Debugf("%d start election ", rf.id)

	rf.IncrGrantedVotes()
	rf.votedFor = int64(rf.id)
	voteReq := &pb.RequestVoteRequest{
		Term:         rf.curTerm,
		CandidateId:  int64(rf.id),
		LastLogIndex: int64(rf.logs.GetLast().Index),
		LastLogTerm:  int64(rf.logs.GetLast().Term),
	}
	rf.PersistRaftState()
	for _, peer := range rf.peers {
		if int64(peer.id) == rf.id {
			continue
		}
		go func(peer *RaftPeerNode) {
			logger.ELogger().Sugar().Debugf("send request vote to %s %s", peer.addr, voteReq.String())
			requestVoteResp, err := (*peer.raftServiceCli).RequestVote(context.Background(), voteReq)
			if err != nil {
				logger.ELogger().Sugar().Errorf("send request vote to %s failed %v", peer.addr, err.Error())
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if requestVoteResp != nil && rf.curTerm == voteReq.Term && rf.role == NodeRoleCandidate {
				logger.ELogger().Sugar().Errorf("send request vote to %s recive -> %s, curterm %d, req term %d", peer.addr, requestVoteResp.String(), rf.curTerm, voteReq.Term)
				if requestVoteResp.VoteGranted {
					// success granted the votes
					rf.IncrGrantedVotes()
					if rf.grantedVotes > len(rf.peers)/2 {
						logger.ELogger().Sugar().Debugf("I'm win this term, (node %d) get majority votes int term %d ", rf.id, rf.curTerm)
						rf.SwitchRaftNodeRole(NodeRoleLeader)
						rf.BroadcastHeartbeat()
						rf.grantedVotes = 0
					}
				} else if requestVoteResp.Term > rf.curTerm {
					// request vote reject
					rf.SwitchRaftNodeRole(NodeRoleFollower)
					rf.curTerm, rf.votedFor = requestVoteResp.Term, -1
					rf.PersistRaftState()
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
		if peer.id == uint64(rf.id) {
			continue
		}
		rf.replicatorCond[peer.id].Signal()
	}
}

// BroadcastHeartbeat broadcast heartbeat to peers
func (rf *Raft) BroadcastHeartbeat() {
	for _, peer := range rf.peers {
		if int64(peer.id) == rf.id {
			continue
		}
		logger.ELogger().Sugar().Debugf("send heart beat to %s", peer.addr)
		go func(peer *RaftPeerNode) {
			rf.replicateOneRound(peer)
		}(peer)
	}
}

// Tick raft heart, this ticket trigger raft main flow running
func (rf *Raft) Tick() {
	for !rf.IsKilled() {
		select {
		case <-rf.electionTimer.C:
			{
				rf.SwitchRaftNodeRole(NodeRoleCandidate)
				rf.IncrCurrentTerm()
				rf.Election()
				rf.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElecTimeout))))
			}
		case <-rf.heartbeatTimer.C:
			{
				if rf.role == NodeRoleLeader {
					rf.BroadcastHeartbeat()
					rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartBeatTimeout))
				}
			}
		}
	}
}

//
// Propose the interface to the appplication propose a operation
//

func (rf *Raft) Propose(payload []byte, cliId int64) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != NodeRoleLeader {
		return -1, -1, false
	}
	if rf.isSnapshoting {
		return -1, -1, false
	}
	newLog := rf.Append(payload, cliId)
	rf.BroadcastAppend()
	return int(newLog.Index), int(newLog.Term), true
}

//
// Append append a new command to it's logs
//

func (rf *Raft) Append(command []byte, cliId int64) *pb.Entry {
	lastLog := rf.logs.GetLast()
	newLog := &pb.Entry{
		Index:    lastLog.Index + 1,
		Term:     uint64(rf.curTerm),
		Data:     command,
		Clientid: cliId,
	}
	rf.logs.Append(newLog)
	rf.matchIdx[rf.id] = int(newLog.Index)
	rf.nextIdx[rf.id] = int(newLog.Index) + 1
	rf.PersistRaftState()
	return newLog
}

// CloseEndsConn close rpc client connect
func (rf *Raft) CloseEndsConn() {
	for _, peer := range rf.peers {
		peer.CloseAllConn()
	}
}

// Replicator manager duplicate run
func (rf *Raft) Replicator(peer *RaftPeerNode) {
	rf.replicatorCond[peer.id].L.Lock()
	defer rf.replicatorCond[peer.id].L.Unlock()
	for !rf.IsKilled() {
		logger.ELogger().Sugar().Debug("peer id wait for replicating...")
		for !(rf.role == NodeRoleLeader && rf.matchIdx[peer.id] < int(rf.logs.GetLast().Index)) {
			rf.replicatorCond[peer.id].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

// replicateOneRound duplicate log entries to other nodes in the cluster
func (rf *Raft) replicateOneRound(peer *RaftPeerNode) {
	rf.mu.RLock()
	if rf.role != NodeRoleLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := uint64(rf.nextIdx[peer.id] - 1)
	logger.ELogger().Sugar().Debugf("leader prev log index %d", prevLogIndex)
	if prevLogIndex < uint64(rf.logs.GetFirst().Index) {
		firstLog := rf.logs.GetFirst()

		snapShotReq := &pb.InstallSnapshotRequest{
			Term:              rf.curTerm,
			LeaderId:          int64(rf.id),
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  int64(firstLog.Term),
		}

		rf.mu.RUnlock()

		logger.ELogger().Sugar().Debugf("send snapshot to %s with %s", peer.addr, snapShotReq.String())

		snapshotResp, err := (*peer.raftServiceCli).Snapshot(context.Background(), snapShotReq)
		if err != nil {
			logger.ELogger().Sugar().Errorf("send snapshot to %s failed %v", peer.addr, err.Error())
		}

		rf.mu.Lock()
		logger.ELogger().Sugar().Debugf("send snapshot to %s with resp %s", peer.addr, snapshotResp.String())

		if snapshotResp != nil {
			if rf.role == NodeRoleLeader && rf.curTerm == snapShotReq.Term {
				if snapshotResp.Term > rf.curTerm {
					rf.SwitchRaftNodeRole(NodeRoleFollower)
					rf.curTerm = snapshotResp.Term
					rf.votedFor = -1
					rf.PersistRaftState()
				} else {
					logger.ELogger().Sugar().Debugf("set peer %d matchIdx %d", peer.id, snapShotReq.LastIncludedIndex)
					rf.matchIdx[peer.id] = int(snapShotReq.LastIncludedIndex)
					rf.nextIdx[peer.id] = int(snapShotReq.LastIncludedIndex) + 1
				}
			}
		}
		rf.mu.Unlock()
	} else {
		firstIndex := rf.logs.GetFirst().Index
		logger.ELogger().Sugar().Debugf("first log index %d", firstIndex)
		newEnts, _ := rf.logs.EraseBefore(int64(prevLogIndex)+1, false)
		entries := make([]*pb.Entry, len(newEnts))
		copy(entries, newEnts)

		appendEntReq := &pb.AppendEntriesRequest{
			Term:         rf.curTerm,
			LeaderId:     int64(rf.id),
			PrevLogIndex: int64(prevLogIndex),
			PrevLogTerm:  int64(rf.logs.GetEntry(int64(prevLogIndex)).Term),
			Entries:      entries,
			LeaderCommit: rf.commitIdx,
		}
		rf.mu.RUnlock()

		// send empty ae to peers
		resp, err := (*peer.raftServiceCli).AppendEntries(context.Background(), appendEntReq)
		if err != nil {
			logger.ELogger().Sugar().Errorf("send append entries to %s failed %v\n", peer.addr, err.Error())
		}
		if rf.role == NodeRoleLeader && rf.curTerm == appendEntReq.Term {
			if resp != nil {
				// deal with appendRnt resp
				if resp.Success {
					logger.ELogger().Sugar().Debugf("send heart beat to %s success", peer.addr)
					rf.matchIdx[peer.id] = int(appendEntReq.PrevLogIndex) + len(appendEntReq.Entries)
					rf.nextIdx[peer.id] = rf.matchIdx[peer.id] + 1
					rf.advanceCommitIndexForLeader()
				} else {
					// there is a new leader in group
					if resp.Term > rf.curTerm {
						rf.SwitchRaftNodeRole(NodeRoleFollower)
						rf.curTerm = resp.Term
						rf.votedFor = VOTE_FOR_NO_ONE
						rf.PersistRaftState()
					} else if resp.Term == rf.curTerm {
						rf.nextIdx[peer.id] = int(resp.ConflictIndex)
						if resp.ConflictTerm != -1 {
							for i := appendEntReq.PrevLogIndex; i >= int64(prevLogIndex); i-- {
								if rf.logs.GetEntry(i).Term == uint64(resp.ConflictTerm) {
									rf.nextIdx[peer.id] = int(i + 1)
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
	for !rf.IsKilled() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIdx {
			logger.ELogger().Sugar().Debug("applier ...")
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIdx, rf.lastApplied
		entries := make([]*pb.Entry, commitIndex-lastApplied)
		copy(entries, rf.logs.GetRange(lastApplied+1, commitIndex))
		logger.ELogger().Sugar().Debugf("%d, applies entries %d-%d in term %d", rf.id, rf.lastApplied, commitIndex, rf.curTerm)

		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- &pb.ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandTerm:  int64(entry.Term),
				CommandIndex: int64(entry.Index),
				Clientid:     entry.Clientid,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = int64(Max(int(rf.lastApplied), int(commitIndex)))
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartSnapshot(snap_idx uint64) error {
	rf.isSnapshoting = true
	if snap_idx <= rf.logs.GetFirstLogId() {
		rf.isSnapshoting = false
		return errors.New("ety index is larger than the first log index")
	}
	rf.logs.EraseBefore(int64(snap_idx), true)
	rf.logs.ResetFirstLogEntry(rf.curTerm, int64(snap_idx))

	// create checkpoint for db
	rf.isSnapshoting = false
	return nil
}
