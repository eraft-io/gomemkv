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
	"log"
	"net"
)

type ServerStat struct {
	CommandsProcessed uint64
	ExpiredKeys       uint64
	MaxUsedMemory     uint64
	TotalClients      uint64
}

type MemKvServer struct {
	// Network port
	port string
	clis []*MemkvClient
}

func MakeDefaultMemKvServer() *MemKvServer {
	return &MemKvServer{
		port: ":12306",
		clis: []*MemkvClient{},
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

	for {

		newConn, err := listener.AcceptTCP()
		if err != nil {
			log.Default().Printf("listener accept connection error %v", err.Error())
			continue
		}

		newCli := MakeMemkvClient(newConn)
		s.clis = append(s.clis, newCli)

		go newCli.ProccessRequest()

	}

}
