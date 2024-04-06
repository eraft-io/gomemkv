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
	"log"
	"net"
	"sync"

	"github.com/eraft-io/gomemkv/engine"
	pb "github.com/eraft-io/gomemkv/raftpb"
	"github.com/eraft-io/gomemkv/replication"
)

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
	clis          []*MemkvClient
	db            engine.IHash
	mu            sync.RWMutex
	rf            *replication.Raft
	applyCh       chan *pb.ApplyMsg
	lastAppliedId int
	notifyChans   map[int]chan *pb.CommandResponse
	pb.UnimplementedRaftServiceServer
}

func MakeDefaultMemKvServer() *MemKvServer {
	return &MemKvServer{
		port: ":12306",
		clis: []*MemkvClient{},
		db:   engine.MakeSkipListHash(),
	}
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

func (s *MemKvServer) getNotifyChan(index int) chan *pb.CommandResponse {
	if _, ok := s.notifyChans[index]; !ok {
		s.notifyChans[index] = make(chan *pb.CommandResponse, 1)
	}
	return s.notifyChans[index]
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

	for {

		newConn, err := listener.AcceptTCP()
		if err != nil {
			log.Default().Printf("listener accept connection error %v", err.Error())
			continue
		}

		newCli := MakeMemkvClient(newConn, s)
		newCli.conn.SetNoDelay(false)
		go newCli.ProccessRequest()
		s.clis = append(s.clis, newCli)

	}

}
