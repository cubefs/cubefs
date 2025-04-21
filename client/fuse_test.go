package main

import (
	"os"
	"syscall"
	"testing"

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
