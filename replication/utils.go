package replication

import (
	"math/rand"
	"time"
)

func RandUInt64Range(min uint64, max uint64) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(int(max-min)) + int(min)
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
