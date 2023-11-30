package cmd

import "testing"

func Test_hostLimit(t *testing.T) {
	h1 := "test h1"
	getToken(h1)
	releaseToken(h1)
	clearChan()

	cntLimit = 2
	getToken(h1)
	releaseToken(h1)

	getToken(h1)
	getToken(h1)
	releaseToken(h1)
	releaseToken(h1)
}
