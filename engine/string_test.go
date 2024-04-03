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

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoString(t *testing.T) {
	go_str := MakeGoString()
	assert.Equal(t, 0, go_str.Len())
	go_str.AppendString("test")
	assert.Equal(t, 4, go_str.Len())
	go_str.Clear()
	assert.Equal(t, 0, go_str.Len())
	new_go_str := MakeGoStringFromByteSlice([]byte("test2"))
	go_str.AppendGoString(new_go_str)
	assert.Equal(t, go_str.Len(), 5)
	split_str := go_str.Range(0, 2)
	assert.Equal(t, split_str.Len(), 2)
	assert.Equal(t, split_str.ToString(), "te")
	assert.Equal(t, "TE", split_str.ToUpper().ToString())
	upper_str := MakeGoStringFromByteSlice([]byte("TEST_DEMO"))
	assert.Equal(t, "test_demo", upper_str.ToLower().ToString())
	a_str := MakeGoStringFromByteSlice([]byte("Hello World"))
	a_str.SetRange(6, "GoMemKv")
	assert.Equal(t, "Hello GoMemKv", a_str.ToString())
}
