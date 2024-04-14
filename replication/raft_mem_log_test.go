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
	"testing"

	pb "github.com/eraft-io/gomemkv/raftpb"
	"github.com/stretchr/testify/assert"
)

func TestMemLogInit(t *testing.T) {
	memLog := MakeMemLog()
	assert.Equal(t, memLog.firstId, uint64(0), "")
	firstEnt := memLog.GetFirst()
	assert.Equal(t, firstEnt.Index, int64(0), "")
	lastEnt := memLog.GetLast()
	assert.Equal(t, lastEnt.Index, int64(0), "")
}

func TestMemLogGetRange(t *testing.T) {
	memLog := MakeMemLog()
	memLog.Append(&pb.Entry{
		Index: 1,
		Term:  1,
		Data:  []byte{0x01, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 2,
		Term:  1,
		Data:  []byte{0x02, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 3,
		Term:  1,
		Data:  []byte{0x03, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 4,
		Term:  1,
		Data:  []byte{0x04, 0x01},
	})

	ents := memLog.GetRange(2, 3)
	for _, ent := range ents {
		t.Logf("got ent %s", ent.String())
	}

}

func TestMemLogGetRangeAfterGC(t *testing.T) {
	memLog := MakeMemLog()
	memLog.Append(&pb.Entry{
		Index: 1,
		Term:  1,
		Data:  []byte{0x01, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 2,
		Term:  1,
		Data:  []byte{0x02, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 3,
		Term:  1,
		Data:  []byte{0x03, 0x01},
	})
	memLog.Append(&pb.Entry{
		Index: 4,
		Term:  1,
		Data:  []byte{0x04, 0x01},
	})

	memLog.EraseBefore(2, true)
	ents := memLog.GetRange(2, 3)
	for _, ent := range ents {
		t.Logf("got ent %s", ent.String())
	}
}
