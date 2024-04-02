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
