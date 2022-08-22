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
	"hash/crc32"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
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

func (loader *VolumeLoader) Release(volName string) {
	loader.volMu.Lock()
	vol, has := loader.volumes[volName]
	if has {
		delete(loader.volumes, volName)
		log.LogDebugf("Release: release volume: volume(%v)", volName)
	}
	loader.volMu.Unlock()
	if has && vol != nil {
		if closeErr := vol.Close(); closeErr != nil {
			log.LogErrorf("Release: close volume fail: volume(%v) err(%v)", volName, closeErr)
		}
	}
}

func (loader *VolumeLoader) Volume(volName string) (*Volume, error) {
	return loader.loadVolume(volName)
}

func (loader *VolumeLoader) syncVolumeInit(volume string) (releaseFunc func()) {
	value, _ := loader.volInitMap.LoadOrStore(volume, new(sync.Mutex))
	var initMu = value.(*sync.Mutex)
	initMu.Lock()
	log.LogDebugf("syncVolumeInit: get volume init lock: volume(%v)", volume)
	return func() {
		initMu.Unlock()
		loader.volInitMap.Delete(volume)
		log.LogDebugf("syncVolumeInit: release volume init lock: volume(%v)", volume)
	}
}

func (loader *VolumeLoader) loadVolume(volName string) (*Volume, error) {
	var err error
	// Check if the volume is on the blacklist.
	if val, exist := loader.blacklist.Load(volName); exist {
		if ts, is := val.(time.Time); is {
			if time.Since(ts) <= volumeBlacklistTTL {
				return nil, proto.ErrVolNotExists
			}
		}
	}

	var volume *Volume
	var exist bool
	loader.volMu.RLock()
	volume, exist = loader.volumes[volName]
	loader.volMu.RUnlock()
	if !exist {
		var release = loader.syncVolumeInit(volName)
		loader.volMu.RLock()
		volume, exist = loader.volumes[volName]
		if exist {
			loader.volMu.RUnlock()
			release()
			return volume, nil
		}
		loader.volMu.RUnlock()

		var onAsyncTaskError AsyncTaskErrorFunc = func(err error) {
			switch err {
			case proto.ErrVolNotExists:
				loader.Release(volName)
				// Add to blacklist
				loader.blacklist.Store(volName, time.Now())
			default:
			}
		}
		var config = &VolumeConfig{
			Volume:           volName,
			Masters:          loader.masters,
			Store:            loader.store,
			OnAsyncTaskError: onAsyncTaskError,
			MetaStrict:       loader.metaStrict,
		}
		if volume, err = NewVolume(config); err != nil {
			if err != proto.ErrVolNotExists {
				log.LogErrorf("loadVolume: init volume fail: volume(%v) err(%v)", volume, err)
			}
			release()
			// Add to blacklist
			loader.blacklist.Store(volName, time.Now())
			return nil, err
		}
		ak, sk := volume.OSSSecure()
		log.LogDebugf("[loadVolume] load Volume: Name[%v] AccessKey[%v] SecretKey[%v]", volName, ak, sk)

		loader.volMu.Lock()
		loader.volumes[volName] = volume
		loader.volMu.Unlock()
		release()

	}

	return volume, nil
}

// Release all
func (loader *VolumeLoader) Close() {
	loader.closeOnce.Do(func() {
		loader.volMu.Lock()
		defer loader.volMu.Unlock()
		for volKey, vol := range loader.volumes {
			_ = vol.Close()
			log.LogDebugf("release Volume %v", volKey)
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

func (m *VolumeManager) Volume(volName string) (*Volume, error) {
	return m.selectLoader(volName).Volume(volName)
}

// Release all
func (m *VolumeManager) Close() {
	m.closeOnce.Do(func() {
		for _, loader := range m.loaders {
			loader.Close()
		}
	})
}

func (m *VolumeManager) Release(volName string) {
	m.selectLoader(volName).Release(volName)
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
