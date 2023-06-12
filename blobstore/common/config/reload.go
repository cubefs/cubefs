package config

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

var r *Reload

type Reload struct {
	reloadFunc func(conf []byte) error
}

func New() *Reload {
	r := &Reload{
		reloadFunc: func(conf []byte) error {
			return nil
		},
	}
	return r
}

func (r *Reload) register(reloadFunc func(conf []byte) error) {
	r.reloadFunc = reloadFunc
}

func Register(fn func(conf []byte) error) {
	r.register(fn)
}

func init() {
	r = New()
}

func HotReload(ctx context.Context, confName string) {
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGUSR1)
	go func(path string) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s:
				conf, err := os.ReadFile(path)
				if err != nil {
					log.Errorf("reload fail to read config file, filename: %s, err: %v", path, err)
					continue
				}
				if err = r.reloadFunc(conf); err != nil {
					log.Errorf("reload config error: %v", err)
				}
			}
		}
	}(confName)
}
