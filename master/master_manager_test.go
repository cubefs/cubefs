package master

import "testing"

func TestHandleLeaderChange(t *testing.T) {
	leaderID := server.id
	newLeaderID := leaderID + 1
	server.handleLeaderChange(newLeaderID)
	if server.metaReady != false {
		t.Errorf("logic error,metaReady should be false,metaReady[%v]", server.metaReady)
		return
	}
	server.handleLeaderChange(leaderID)
	if server.metaReady == false {
		t.Errorf("logic error,metaReady should be true,metaReady[%v]", server.metaReady)
		return
	}
}
