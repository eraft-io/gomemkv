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

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	pb "github.com/eraft-io/gomemkv/raftpb"
	"github.com/eraft-io/gomemkv/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: server [nodeId]")
		return
	}
	nodeIdStr := os.Args[1]
	nodeId, err := strconv.Atoi(nodeIdStr)
	if err != nil {
		panic(err)
	}
	memKvServer := server.MakeDefaultMemKvServer(nodeId)
	lis, err := net.Listen("tcp", server.DefaultPeersMap[nodeId])
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer()
	pb.RegisterRaftServiceServer(s, memKvServer)
	reflection.Register(s)
	go memKvServer.Boot()
	fmt.Printf("server listen on: %s \n", server.DefaultPeersMap[nodeId])
	s.Serve(lis)
}
