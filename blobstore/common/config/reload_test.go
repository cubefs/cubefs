package config

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReload(t *testing.T) {
	// test load
	var config interface{}
	err := LoadFile(&config, srcconf)
	require.NoError(t, err)

	var reload []byte
	ctx, cancel := context.WithCancel(context.Background())
	HotReload(ctx, srcconf)
	time.Sleep(200 * time.Millisecond)

	// test reload success
	Register(func(conf []byte) error {
		reload = append(reload, conf...)
		return nil
	})
	pid := syscall.Getpid()
	p, err := os.FindProcess(pid)
	if err != nil {
		t.Log("find process failed, err: ", err)
	}
	err = p.Signal(syscall.SIGUSR1)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Greater(t, len(reload), 0)

	// test reload failed
	reload = make([]byte, 0, 5)
	Register(func(conf []byte) error {
		return errors.New("some error occurred")
	})
	err = p.Signal(syscall.SIGUSR1)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, len(reload), 0)

	cancel()
}
