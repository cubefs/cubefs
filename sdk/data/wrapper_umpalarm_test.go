package data

import "testing"

func TestHandleUmpAlarm(t *testing.T) {
	cluster := "chubaofs01"
	vol := "ltptest"
	act := "test act"
	msg := "this is a message"
	handleUmpAlarm(cluster, vol, act, msg)
}