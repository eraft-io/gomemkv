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
	"log"

	"github.com/eraft-io/gomemkv/raftpb"
	"google.golang.org/grpc"
)

type PeerNode struct {
	id      uint64
	address string
	conns   []*grpc.ClientConn
	rpcCli  *raftpb.RaftServiceClient
}

func (p *PeerNode) Id() uint64 {
	return p.id
}

func (p *PeerNode) RaftRpcCli() *raftpb.RaftServiceClient {
	return p.rpcCli
}

func MakePeerNode(addr string, id uint64) *PeerNode {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Default().Fatalf("faild to connect to %s: %s", addr, err.Error())
	}
	conns := []*grpc.ClientConn{}
	conns = append(conns, conn)
	rpcCli := raftpb.NewRaftServiceClient(conn)
	return &PeerNode{
		id:      id,
		address: addr,
		conns:   conns,
		rpcCli:  &rpcCli,
	}
}
