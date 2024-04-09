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
	goStr := MakeGoString()
	assert.Equal(t, 0, goStr.Len())
	goStr.AppendString("test")
	assert.Equal(t, 4, goStr.Len())
	goStr.Clear()
	assert.Equal(t, 0, goStr.Len())
	newGoStr := MakeGoStringFromByteSlice([]byte("test2"))
	goStr.AppendGoString(newGoStr)
	assert.Equal(t, goStr.Len(), 5)
	splitStr := goStr.Range(0, 2)
	assert.Equal(t, splitStr.Len(), 2)
	assert.Equal(t, splitStr.ToString(), "te")
	assert.Equal(t, "TE", splitStr.ToUpper().ToString())
	upperStr := MakeGoStringFromByteSlice([]byte("TEST_DEMO"))
	assert.Equal(t, "test_demo", upperStr.ToLower().ToString())
	aStr := MakeGoStringFromByteSlice([]byte("Hello World"))
	aStr.SetRange(6, "GoMemKv")
	assert.Equal(t, "Hello GoMemKv", aStr.ToString())
}
