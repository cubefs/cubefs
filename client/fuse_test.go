package main

import (
	"os"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/require"
)

func TestRegisterInterceptedSignal(t *testing.T) {
	mnt := "test-mount"
	receivedExitSignal := false
	sigRegister := make(chan interface{}, 1)
	registerInterceptedSignal(mnt, func(sig bool) bool {
		receivedExitSignal = sig
		sigRegister <- true
		return true
	})

	err := syscall.Kill(os.Getpid(), syscall.SIGBUS)
	if err != nil {
		t.Errorf("Failed to send SIGINT signal: %v", err)
	}
	<-sigRegister

	require.NoError(t, err)
	require.Equal(t, receivedExitSignal, true)
}

func TestParseMountOption_AheadReadFollowerRead(t *testing.T) {
	cfg := config.LoadConfigString(`{
		"mountPoint": "/tmp/mnt",
		"volName": "test-vol",
		"owner": "test-owner",
		"masterAddr": "127.0.0.1:17010",
		"aheadReadFollowerRead": "true"
	}`)
	opt, err := parseMountOption(cfg)
	require.NoError(t, err)
	require.True(t, opt.AheadReadFollowerRead)
}
