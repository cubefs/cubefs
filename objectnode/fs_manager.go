// Copyright 2019 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

const (
	volumeBlacklistCleanupInterval = time.Minute * 1
	volumeBlacklistTTL             = time.Second * 10
	volumeLoaderNum                = 4
)

type VolumeLoader struct {
	masters    []string
	store      Store              // Storage for ACP management
	volumes    map[string]*Volume // mapping: volume name -> *Volume
	volMu      sync.RWMutex
	volInitMap sync.Map // mapping: volume name -> *sync.Mutex
	blacklist  sync.Map // mapping: volume name -> timestamp (time.Time)
	closeOnce  sync.Once
	closeCh    chan struct{}
	metaStrict bool
}

func (loader *VolumeLoader) blacklistCleanup() {
	t := time.NewTimer(volumeBlacklistCleanupInterval)
	for {
		select {
		case <-t.C:
		case <-loader.closeCh:
			t.Stop()
			return
		}
		loader.blacklist.Range(func(key, value interface{}) bool {
			ts, is := value.(time.Time)
			if !is || time.Since(ts) > volumeBlacklistTTL {
				loader.blacklist.Delete(key)
			}
			return true
		})
		t.Reset(volumeBlacklistCleanupInterval)
	}
}

func (loader *VolumeLoader) Release(ctx context.Context, volName string) {
	span := spanWithOperation(ctx, "ReleaseVolume")

	loader.volMu.Lock()
	vol, has := loader.volumes[volName]
	if has {
		delete(loader.volumes, volName)
	}
	loader.volMu.Unlock()
	if has && vol != nil {
		if closeErr := vol.Close(); closeErr != nil {
			span.Errorf("volume close fail: volume(%v) err(%v)", volName, closeErr)
		}
	}
}

func (loader *VolumeLoader) Volume(ctx context.Context, volName string) (*Volume, error) {
	return loader.loadVolume(ctx, volName)
}

func (loader *VolumeLoader) VolumeWithoutBlacklist(ctx context.Context, volName string) (*Volume, error) {
	return loader.loadVolumeWithoutBlacklist(ctx, volName)
}

func (loader *VolumeLoader) syncVolumeInit(volume string) (releaseFunc func()) {
	value, _ := loader.volInitMap.LoadOrStore(volume, new(sync.Mutex))
	initMu := value.(*sync.Mutex)
	initMu.Lock()

	return func() {
		initMu.Unlock()
		loader.volInitMap.Delete(volume)
	}
}

func (loader *VolumeLoader) loadVolumeWithoutBlacklist(ctx context.Context, volName string) (volume *Volume, err error) {
	span := spanWithOperation(ctx, "LoadVolume")

	loader.volMu.RLock()
	volume, exist := loader.volumes[volName]
	loader.volMu.RUnlock()
	if !exist {
		release := loader.syncVolumeInit(volName)
		defer release()

		loader.volMu.RLock()
		volume, exist = loader.volumes[volName]
		if exist {
			loader.volMu.RUnlock()
			return
		}
		loader.volMu.RUnlock()

		var onAsyncTaskError AsyncTaskErrorFunc = func(err error) {
			switch err {
			case proto.ErrVolNotExists:
				loader.Release(ctx, volName)
			default:
			}
		}
		config := &VolumeConfig{
			Volume:           volName,
			Masters:          loader.masters,
			Store:            loader.store,
			OnAsyncTaskError: onAsyncTaskError,
			MetaStrict:       loader.metaStrict,
		}
		if volume, err = NewVolume(ctx, config); err != nil {
			span.Debugf("new volume fail: volume(%v) err(%v)", volName, err)
			if err != proto.ErrVolNotExists {
				span.Errorf("new volume fail: volume(%v) config(%+v) err(%v)", volume, config, err)
			}
			return
		}

		loader.volMu.Lock()
		loader.volumes[volName] = volume
		loader.volMu.Unlock()

		ak, sk := volume.OSSSecure()
		span.Debugf("new volume success: volume(%+v) accessKey(%v) secretKey(%v)", volume, ak, sk)
	}

	return
}

func (loader *VolumeLoader) loadVolume(ctx context.Context, volName string) (volume *Volume, err error) {
	span := spanWithOperation(ctx, "LoadVolume")
	// Check if the volume is on the blacklist.
	if val, exist := loader.blacklist.Load(volName); exist {
		if ts, is := val.(time.Time); is {
			if time.Since(ts) <= volumeBlacklistTTL {
				span.Debugf("load volume(%v) from blacklist and not expired, return ErrVolNotExists", volName)
				err = proto.ErrVolNotExists
				return
			}
		}
	}

	loader.volMu.RLock()
	volume, exist := loader.volumes[volName]
	loader.volMu.RUnlock()
	if !exist {
		release := loader.syncVolumeInit(volName)
		defer release()

		loader.volMu.RLock()
		volume, exist = loader.volumes[volName]
		if exist {
			loader.volMu.RUnlock()
			return
		}
		loader.volMu.RUnlock()

		var onAsyncTaskError AsyncTaskErrorFunc = func(err error) {
			switch err {
			case proto.ErrVolNotExists:
				loader.Release(ctx, volName)
				// Add to blacklist
				loader.blacklist.Store(volName, time.Now())
			default:
			}
		}
		config := &VolumeConfig{
			Volume:           volName,
			Masters:          loader.masters,
			Store:            loader.store,
			OnAsyncTaskError: onAsyncTaskError,
			MetaStrict:       loader.metaStrict,
		}
		if volume, err = NewVolume(ctx, config); err != nil {
			span.Debugf("new volume fail and add to blacklist: volume(%v) err(%v)", volName, err)
			if err != proto.ErrVolNotExists {
				span.Errorf("new volume fail and add to blacklist: volume(%v) config(%+v) err(%v)",
					volume, config, err)
			}
			// Add to blacklist
			loader.blacklist.Store(volName, time.Now())
			return
		}

		loader.volMu.Lock()
		loader.volumes[volName] = volume
		loader.volMu.Unlock()

		ak, sk := volume.OSSSecure()
		span.Debugf("new volume success: volume(%+v) accessKey(%v) secretKey(%v)", volume, ak, sk)
	}

	return
}

// Release all
func (loader *VolumeLoader) Close() {
	loader.closeOnce.Do(func() {
		loader.volMu.Lock()
		defer loader.volMu.Unlock()
		for _, vol := range loader.volumes {
			_ = vol.Close()
		}
		loader.volumes = make(map[string]*Volume)
		close(loader.closeCh)
	})
}

func NewVolumeLoader(masters []string, store Store, strict bool) *VolumeLoader {
	loader := &VolumeLoader{
		masters:    masters,
		store:      store,
		volumes:    make(map[string]*Volume),
		closeCh:    make(chan struct{}),
		metaStrict: strict,
	}
	go loader.blacklistCleanup()
	return loader
}

type VolumeManager struct {
	masters    []string
	mc         *master.MasterClient
	loaders    [volumeLoaderNum]*VolumeLoader
	store      Store
	metaStrict bool
	closeOnce  sync.Once
	closeCh    chan struct{}
}

func (m *VolumeManager) selectLoader(name string) *VolumeLoader {
	i := crc32.ChecksumIEEE([]byte(name)) % volumeLoaderNum
	return m.loaders[i]
}

func (m *VolumeManager) Volume(ctx context.Context, volName string) (*Volume, error) {
	return m.selectLoader(volName).Volume(ctx, volName)
}

func (m *VolumeManager) VolumeWithoutBlacklist(ctx context.Context, volName string) (*Volume, error) {
	return m.selectLoader(volName).VolumeWithoutBlacklist(ctx, volName)
}

// Release all
func (m *VolumeManager) Close() {
	m.closeOnce.Do(func() {
		for _, loader := range m.loaders {
			loader.Close()
		}
	})
}

func (m *VolumeManager) Release(ctx context.Context, volName string) {
	m.selectLoader(volName).Release(ctx, volName)
}

func (m *VolumeManager) init() {
	m.store = &xattrStore{
		vm: m,
	}
	for i := 0; i < len(m.loaders); i++ {
		m.loaders[i] = NewVolumeLoader(m.masters, m.store, m.metaStrict)
	}
}

func NewVolumeManager(masters []string, strict bool) *VolumeManager {
	manager := &VolumeManager{
		masters:    masters,
		closeCh:    make(chan struct{}),
		metaStrict: strict,
	}
	manager.init()
	return manager
}
