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

package server

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/eraft-io/gomemkv/engine"
	"github.com/eraft-io/gomemkv/logger"
	pb "github.com/eraft-io/gomemkv/raftpb"
	"github.com/eraft-io/gomemkv/replication"
	"github.com/eraft-io/gomemkv/storage"
)

// for demo test
var DefaultPeersMap = map[int]string{0: ":8088", 1: ":8089", 2: ":8090"}
var DefaultPeerClientsMap = map[int]string{0: ":12306", 1: ":12307", 2: ":12308"}

type ServerStat struct {
	CommandsProcessed uint64
	ExpiredKeys       uint64
	MaxUsedMemory     uint64
	TotalClients      uint64
}

type MemKvServer struct {
	// Network port
	port          string
	grpcPort      string
	dead          int32
	cliId         int
	clis          map[int]*MemkvClient
	db            engine.IHash
	mu            sync.RWMutex
	rf            *replication.Raft
	applyCh       chan *pb.ApplyMsg
	stopApplyCh   chan interface{}
	lastAppliedId int
	notifyChans   map[int]chan *pb.CommandResponse
	pb.UnimplementedRaftServiceServer
}

func MakeDefaultMemKvServer(nodeId int) *MemKvServer {
	peerEnds := []*replication.RaftPeerNode{}
	for id, addr := range DefaultPeersMap {
		newEnd := replication.MakeRaftPeerNode(addr, uint64(id))
		peerEnds = append(peerEnds, newEnd)
	}
	newApplyCh := make(chan *pb.ApplyMsg)
	logdb_eng := storage.EngineFactory("leveldb", "./data/log_"+strconv.Itoa(nodeId))

	newRf := replication.MakeRaft(peerEnds, int64(nodeId), logdb_eng, newApplyCh, 2000, 6000)
	kvSvr := &MemKvServer{
		rf:            newRf,
		port:          DefaultPeerClientsMap[nodeId],
		clis:          map[int]*MemkvClient{},
		db:            engine.MakeSkipListHash(),
		grpcPort:      DefaultPeersMap[nodeId],
		dead:          0,
		lastAppliedId: 0,
		applyCh:       newApplyCh,
		cliId:         0,
		notifyChans:   make(map[int]chan *pb.CommandResponse),
	}
	kvSvr.stopApplyCh = make(chan interface{})
	return kvSvr
}

func (s *MemKvServer) IsKilled() bool {
	return atomic.LoadInt32(&s.dead) == 1
}

func (s *MemKvServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := &pb.RequestVoteResponse{}
	s.rf.HandleRequestVote(req, resp)
	return resp, nil
}

func (s *MemKvServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := &pb.AppendEntriesResponse{}
	s.rf.HandleAppendEntries(req, resp)
	return resp, nil
}

func (s *MemKvServer) Snapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	resp := &pb.InstallSnapshotResponse{}
	// s.rf.HandleInstallSnapshot(req, resp)
	return resp, nil
}

func (s *MemKvServer) GetNotifyChan(index int) chan *pb.CommandResponse {
	if _, ok := s.notifyChans[index]; !ok {
		s.notifyChans[index] = make(chan *pb.CommandResponse, 1)
	}
	return s.notifyChans[index]
}

func (s *MemKvServer) AppliedAndExecCmd(done <-chan interface{}) {

	for !s.IsKilled() {

		select {
		case <-done:
			return
		case appliedMsg := <-s.applyCh:
			if appliedMsg.CommandValid {
				s.mu.Lock()

				cmdParams := &CmdParams{}
				if err := json.Unmarshal(appliedMsg.Command, &cmdParams); err != nil {
					logger.ELogger().Sugar().Errorf(err.Error())
					continue
				}

				s.lastAppliedId = int(appliedMsg.CommandIndex)

				if s.rf.IsLeader() {
					if len(cmdParams.Params) > 0 {
						replyBuf, _ := s.clis[int(appliedMsg.Clientid)].ExecCmd(cmdParams.Params)
						s.clis[int(appliedMsg.Clientid)].conn.Write(replyBuf)
					}
				} else {
					// mock client
					newCli := MakeMemkvClient(nil, s, 99999)
					newCli.ExecCmd(cmdParams.Params)
				}

				cmdResp := &pb.CommandResponse{}
				ch := s.GetNotifyChan(int(appliedMsg.CommandIndex))
				ch <- cmdResp

				s.mu.Unlock()
			}
		}

	}

}

func (s *MemKvServer) Boot() {

	addr, err := net.ResolveTCPAddr("tcp4", s.port)
	if err != nil {
		log.Fatal(err.Error())
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatal(err.Error())
	}

	go s.AppliedAndExecCmd(s.stopApplyCh)

	for {

		newConn, err := listener.AcceptTCP()
		if err != nil {
			log.Default().Printf("listener accept connection error %v", err.Error())
			continue
		}
		s.cliId++
		newCli := MakeMemkvClient(newConn, s, s.cliId)
		newCli.conn.SetNoDelay(false)
		go newCli.ProccessRequest()
		s.clis[s.cliId] = newCli

	}

}
