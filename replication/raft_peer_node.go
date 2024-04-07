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
	"github.com/eraft-io/gomemkv/logger"
	raftpb "github.com/eraft-io/gomemkv/raftpb"
	"google.golang.org/grpc"
)

type RaftPeerNode struct {
	id             uint64
	addr           string
	conns          []*grpc.ClientConn
	raftServiceCli *raftpb.RaftServiceClient
}

func (rfEnd *RaftPeerNode) Id() uint64 {
	return rfEnd.id
}

func (rfEnd *RaftPeerNode) GetRaftServiceCli() *raftpb.RaftServiceClient {
	return rfEnd.raftServiceCli
}

func MakeRaftPeerNode(addr string, id uint64) *RaftPeerNode {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		logger.ELogger().Sugar().DPanicf("faild to connect: %v", err)
	}
	conns := []*grpc.ClientConn{}
	conns = append(conns, conn)
	rpc_client := raftpb.NewRaftServiceClient(conn)
	return &RaftPeerNode{
		id:             id,
		addr:           addr,
		conns:          conns,
		raftServiceCli: &rpc_client,
	}
}

func (rfEnd *RaftPeerNode) CloseAllConn() {
	for _, conn := range rfEnd.conns {
		conn.Close()
	}
}
