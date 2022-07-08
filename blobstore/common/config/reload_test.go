package config

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

func TestReload(t *testing.T) {
	var config interface{}
	err := LoadFile(&config, srcconf)
	if err != nil {
		t.Logf("Load config error: %s", err)
	}
	HotReload(context.Background(), srcconf)
	time.Sleep(200 * time.Millisecond)
	Register(func(conf []byte) error {
		log.Infof("run reload func,config:%s", conf)
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
}
