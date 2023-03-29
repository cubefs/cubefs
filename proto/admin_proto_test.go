package proto

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataPartitionResponse_GetLeaderAddr(t *testing.T) {
	dpr:=new(DataPartitionResponse)
	leaderAddr:="127.0.0.1:8888"
	dpr.LeaderAddr=NewAtomicString(leaderAddr)
	assert.Equal(t,leaderAddr,dpr.GetLeaderAddr())
}