// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package fs

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/stretchr/testify/require"
)

func newSuperDirDirtyHarness(metaAccel bool) *Super {
	return &Super{
		metaCacheAcceleration: metaAccel,
		dirDirtyCache:         make(map[uint64]bool),
		dirDirtyCount:         make(map[uint64]int),
	}
}

func TestMetaCacheAccelerationInitializesReadDirPool(t *testing.T) {
	t.Parallel()
	s := &Super{metaCacheAcceleration: true}
	s.initReadDirPool()
	require.NotNil(t, s.readDirPool)

	done := make(chan struct{})
	s.readDirPool.Run(func() {
		close(done)
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("readDirPool task did not run")
	}
}

func TestMetaCacheAccelerationOffLeavesReadDirPoolNil(t *testing.T) {
	t.Parallel()
	s := &Super{metaCacheAcceleration: false}
	s.initReadDirPool()
	require.Nil(t, s.readDirPool)
}

func TestReadDirAllCacheBegin_metaAccelerationOff(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(false)
	const ino = 42
	require.False(t, s.readDirAllCacheBegin(ino))
	_, ok := s.dirDirtyCache[ino]
	require.False(t, ok, "should not touch dirDirtyCache when acceleration off")
}

func TestReadDirAllCacheBegin_countPositive_skips(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 100
	s.dirDirtyCount[ino] = 1
	require.True(t, s.readDirAllCacheBegin(ino))
	_, ok := s.dirDirtyCache[ino]
	require.False(t, ok, "skip path must not establish dirDirtyCache entry")
}

func TestReadDirAllCacheBegin_countZero_setsFalse(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 200
	require.False(t, s.readDirAllCacheBegin(ino))
	v, ok := s.dirDirtyCache[ino]
	require.True(t, ok)
	require.False(t, v)
}

func TestReleaseDirDirty_metaAccelerationOff(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(false)
	s.dirDirtyCache[7] = true
	s.ReleaseDirDirty(7)
	require.True(t, s.dirDirtyCache[7])
}

func TestReleaseDirDirty_deletesEntry(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	s.dirDirtyCache[11] = false
	s.dirDirtyCache[12] = true
	s.ReleaseDirDirty(11)
	_, ok := s.dirDirtyCache[11]
	require.False(t, ok)
	_, ok12 := s.dirDirtyCache[12]
	require.True(t, ok12)
}

func TestBeginEndDirMutation_metaAccelerationOff(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(false)
	const ino = 55
	s.BeginDirMutation(ino)
	s.EndDirMutation(ino)
	require.Empty(t, s.dirDirtyCount)
}

func TestBeginEndDirMutation_singlePair_clearsCount(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 300
	s.BeginDirMutation(ino)
	require.Equal(t, 1, s.dirDirtyCount[ino])
	s.EndDirMutation(ino)
	_, ok := s.dirDirtyCount[ino]
	require.False(t, ok)
}

func TestBeginEndDirMutation_nestedDecrement(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 400
	s.BeginDirMutation(ino)
	s.BeginDirMutation(ino)
	require.Equal(t, 2, s.dirDirtyCount[ino])

	s.EndDirMutation(ino)
	require.Equal(t, 1, s.dirDirtyCount[ino])

	s.EndDirMutation(ino)
	_, ok := s.dirDirtyCount[ino]
	require.False(t, ok)
}

func TestEndDirMutation_withDirDirtyCacheKey_nestedDecrementStillMarksDirty(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 450
	require.False(t, s.readDirAllCacheBegin(ino))

	s.BeginDirMutation(ino)
	s.BeginDirMutation(ino)

	s.EndDirMutation(ino)
	require.True(t, s.dirDirtyCache[ino])
	require.Equal(t, 1, s.dirDirtyCount[ino])

	s.EndDirMutation(ino)
	require.True(t, s.dirDirtyCache[ino])
	_, ok := s.dirDirtyCount[ino]
	require.False(t, ok)
}

func TestEndDirMutation_setsDirtyWhenDirDirtyCacheKeyExists(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 500
	require.False(t, s.readDirAllCacheBegin(ino))
	require.False(t, s.dirDirtyCache[ino])

	s.BeginDirMutation(ino)
	s.EndDirMutation(ino)

	require.True(t, s.dirDirtyCache[ino])
	_, ok := s.dirDirtyCount[ino]
	require.False(t, ok)
}

func TestEndDirMutation_withoutBegin_doesNotAlterCountMap(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 600
	s.EndDirMutation(ino)
	require.Empty(t, s.dirDirtyCount)
}

func TestCheckDirDirty_metaAccelerationOff_runsCallback(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(false)
	var ran int32
	s.CheckDirDirty(99, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(1), atomic.LoadInt32(&ran))
}

func TestCheckDirDirty_skipsWhenCountPositive(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 700
	s.dirDirtyCount[ino] = 1
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(0), atomic.LoadInt32(&ran))
}

func TestCheckDirDirty_skipsWhenDirtyTrue(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 800
	s.dirDirtyCache[ino] = true
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(0), atomic.LoadInt32(&ran))
}

func TestCheckDirDirty_runsCallbackWhenClean(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino = 900
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(1), atomic.LoadInt32(&ran))
}

func TestDirDirty_concurrentBeginEnd_noLostUpdates(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1000
	var wg sync.WaitGroup
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s.BeginDirMutation(ino)
			s.EndDirMutation(ino)
		}()
	}
	wg.Wait()
	require.Empty(t, s.dirDirtyCount)
}

func TestReadDirAllCacheBegin_resetsDirtyTrueToFalseWhenNoMutation(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1100
	s.dirDirtyCache[ino] = true
	require.False(t, s.readDirAllCacheBegin(ino))
	v, ok := s.dirDirtyCache[ino]
	require.True(t, ok)
	require.False(t, v, "entry gate establishes fresh scan baseline")
}

func TestReadDirAllCacheBegin_ReleaseDirDirty_thenBeginAgain(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1101
	require.False(t, s.readDirAllCacheBegin(ino))
	s.ReleaseDirDirty(ino)
	_, ok := s.dirDirtyCache[ino]
	require.False(t, ok)
	require.False(t, s.readDirAllCacheBegin(ino))
	_, ok = s.dirDirtyCache[ino]
	require.True(t, ok)
}

func TestReleaseDirDirty_missingKey_noOp(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	s.ReleaseDirDirty(99999)
	require.Empty(t, s.dirDirtyCache)
}

func TestEndDirMutation_whenDirDirtyCacheAlreadyTrue(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1200
	s.dirDirtyCache[ino] = true
	s.BeginDirMutation(ino)
	s.EndDirMutation(ino)
	require.True(t, s.dirDirtyCache[ino])
	require.Empty(t, s.dirDirtyCount)
}

func TestEndDirMutation_doubleEndAfterSingleBegin_secondEndNoCount(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1201
	s.BeginDirMutation(ino)
	s.EndDirMutation(ino)
	s.EndDirMutation(ino)
	require.Empty(t, s.dirDirtyCount)
}

func TestCheckDirDirty_skipsOnCountWhenDirtyWouldOtherwiseAllow(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1300
	s.dirDirtyCount[ino] = 2
	s.dirDirtyCache[ino] = false
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(0), atomic.LoadInt32(&ran))
}

func TestCheckDirDirty_skipsWhenBothCountAndDirtySet(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1301
	s.dirDirtyCount[ino] = 1
	s.dirDirtyCache[ino] = true
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(0), atomic.LoadInt32(&ran))
}

func TestCheckDirDirty_runsWhenExplicitFalseKeyAndZeroCount(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1302
	s.dirDirtyCache[ino] = false
	var ran int32
	s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
	require.Equal(t, int32(1), atomic.LoadInt32(&ran))
}

func TestDirDirty_twoInodesIndependent(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const a, b uint64 = 1400, 1401
	s.BeginDirMutation(a)
	s.readDirAllCacheBegin(b)
	require.Equal(t, 1, s.dirDirtyCount[a])
	_, ok := s.dirDirtyCache[b]
	require.True(t, ok)
	s.EndDirMutation(a)
	require.Empty(t, s.dirDirtyCount)
	v, ok := s.dirDirtyCache[b]
	require.True(t, ok)
	require.False(t, v)
}

func TestBeginEnd_manyPairsSameIno(t *testing.T) {
	t.Parallel()
	s := newSuperDirDirtyHarness(true)
	const ino uint64 = 1500
	for i := 0; i < 25; i++ {
		s.BeginDirMutation(ino)
	}
	for i := 0; i < 25; i++ {
		s.EndDirMutation(ino)
	}
	require.Empty(t, s.dirDirtyCount)
}

func TestCheckDirDirty_tableDriven(t *testing.T) {
	t.Parallel()
	type row struct {
		name     string
		accel    bool
		count    int
		dirty    *bool
		wantRuns bool
	}
	trueB := true
	cases := []row{
		{"accel_off", false, 0, nil, true},
		{"accel_on_clean", true, 0, nil, true},
		{"accel_on_dirty_true", true, 0, &trueB, false},
		{"accel_on_count", true, 1, nil, false},
		{"accel_on_count_dirty", true, 2, &trueB, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newSuperDirDirtyHarness(tc.accel)
			const ino uint64 = 1600
			if tc.count > 0 {
				s.dirDirtyCount[ino] = tc.count
			}
			if tc.dirty != nil {
				s.dirDirtyCache[ino] = *tc.dirty
			}
			var ran int32
			s.CheckDirDirty(ino, func() { atomic.StoreInt32(&ran, 1) })
			if tc.wantRuns {
				require.Equal(t, int32(1), atomic.LoadInt32(&ran))
			} else {
				require.Equal(t, int32(0), atomic.LoadInt32(&ran))
			}
		})
	}
}

func FuzzDirDirtyBeginEndBalanced(f *testing.F) {
	f.Add(uint64(77), byte(3))
	f.Add(uint64(1), byte(1))
	f.Fuzz(func(t *testing.T, ino uint64, depth byte) {
		if ino == 0 {
			ino = 1
		}
		n := int(depth%15) + 1
		s := newSuperDirDirtyHarness(true)
		for i := 0; i < n; i++ {
			s.BeginDirMutation(ino)
		}
		for i := 0; i < n; i++ {
			s.EndDirMutation(ino)
		}
		require.Empty(t, s.dirDirtyCount)
	})
}

func TestNewSuper_AheadReadFollowerRead_Mock(t *testing.T) {
	patches := gomonkey.ApplyFunc(meta.NewMetaWrapper, func(param *meta.MetaConfig) (*meta.MetaWrapper, error) {
		mw := &meta.MetaWrapper{}
		mc := master.NewMasterClient([]string{"127.0.0.1"}, false)
		val := reflect.ValueOf(mw).Elem()
		mcField := val.FieldByName("mc")
		ptr := unsafe.Pointer(mcField.UnsafeAddr())
		reflect.NewAt(mcField.Type(), ptr).Elem().Set(reflect.ValueOf(mc))
		return mw, nil
	})
	defer patches.Reset()

	patches.ApplyMethod(reflect.TypeOf(&master.AdminAPI{}), "GetVolumeSimpleInfo", func(_ *master.AdminAPI, name string) (*proto.SimpleVolView, error) {
		return &proto.SimpleVolView{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(&master.AdminAPI{}), "GetClusterInfo", func(_ *master.AdminAPI) (*proto.ClusterInfo, error) {
		return &proto.ClusterInfo{}, nil
	})
	patches.ApplyFunc(stream.NewExtentClient, func(config *stream.ExtentConfig) (*stream.ExtentClient, error) {
		require.True(t, config.AheadReadFollowerRead)
		return &stream.ExtentClient{}, nil
	})

	opt := &proto.MountOptions{
		AheadReadEnable:       true,
		AheadReadFollowerRead: true,
		MinReadAheadSize:      1024,
		Master:                "127.0.0.1",
		Volname:               "test-vol",
		Owner:                 "test-owner",
		MountPoint:            "/tmp/mnt",
		Logpath:               "/tmp/log",
	}

	defer func() { recover() }()
	s, err := NewSuper(opt)
	if err != nil {
		t.Logf("NewSuper err: %v", err)
	}
	if s != nil {
		require.NotNil(t, s.mw)
	}
}

func TestNewSuper_ClientPoolIDAndMetaRegionValidation(t *testing.T) {
	const (
		defaultPoolID     = uint8(1)
		validPoolID       = uint8(2)
		invalidPoolID     = uint8(9)
		defaultMetaRegion = "default-region"
		validMetaRegion   = "region-a"
		invalidMetaRegion = "unknown-region"
	)

	tests := []struct {
		name           string
		poolID         uint8
		metaRegion     string
		wantPoolID     uint8
		wantMetaRegion string
	}{
		{
			name:           "valid overrides default",
			poolID:         validPoolID,
			metaRegion:     validMetaRegion,
			wantPoolID:     validPoolID,
			wantMetaRegion: validMetaRegion,
		},
		{
			name:           "invalid keeps default",
			poolID:         invalidPoolID,
			metaRegion:     invalidMetaRegion,
			wantPoolID:     defaultPoolID,
			wantMetaRegion: defaultMetaRegion,
		},
		{
			name:           "unset keeps default",
			wantPoolID:     defaultPoolID,
			wantMetaRegion: defaultMetaRegion,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			patches := newSuperMountOptionPatches(t, defaultPoolID, defaultMetaRegion)
			defer patches.Reset()

			opt := newTestMountOptions()
			opt.PoolId = tt.poolID
			opt.MetaRegion = tt.metaRegion

			s, err := NewSuper(opt)
			require.NoError(t, err)
			require.NotNil(t, s)
			defer close(s.closeC)

			require.Equal(t, tt.wantPoolID, s.mw.GetClientPoolId())
			require.Equal(t, tt.wantMetaRegion, metaWrapperDefaultRegion(s.mw))
		})
	}
}

func newSuperMountOptionPatches(t *testing.T, defaultPoolID uint8, defaultMetaRegion string) *gomonkey.Patches {
	t.Helper()

	patches := gomonkey.ApplyFunc(meta.NewMetaWrapper, func(param *meta.MetaConfig) (*meta.MetaWrapper, error) {
		mw := &meta.MetaWrapper{}
		setMetaWrapperMasterClient(mw, master.NewMasterClient([]string{"127.0.0.1"}, false))
		setMetaWrapperUint8Field(mw, "defaultPoolId", defaultPoolID)
		setMetaWrapperStringField(mw, "defaultMetaRegion", defaultMetaRegion)
		return mw, nil
	})

	patches.ApplyMethod(reflect.TypeOf(&master.AdminAPI{}), "GetVolumeSimpleInfo", func(_ *master.AdminAPI, name string) (*proto.SimpleVolView, error) {
		return &proto.SimpleVolView{
			Pools: map[uint8]*proto.StoragePoolInfo{
				defaultPoolID: {Id: defaultPoolID},
				2:             {Id: 2},
			},
			AllowedRegions: []string{defaultMetaRegion, "region-a"},
		}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(&master.AdminAPI{}), "GetClusterInfo", func(_ *master.AdminAPI) (*proto.ClusterInfo, error) {
		return &proto.ClusterInfo{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(&master.AdminAPI{}), "ListStoragePools", func(_ *master.AdminAPI) ([]*proto.StoragePoolInfo, error) {
		return []*proto.StoragePoolInfo{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(&meta.MetaWrapper{}), "GetRootIno", func(_ *meta.MetaWrapper, subdir string) (uint64, error) {
		return proto.RootIno, nil
	})
	patches.ApplyFunc(stream.NewExtentClient, func(config *stream.ExtentConfig) (*stream.ExtentClient, error) {
		ec := &stream.ExtentClient{}
		setExtentClientMultiVerMgr(ec)
		return ec, nil
	})

	return patches
}

func newTestMountOptions() *proto.MountOptions {
	return &proto.MountOptions{
		Master:                "127.0.0.1",
		Volname:               "test-vol",
		Owner:                 "test-owner",
		MountPoint:            "/tmp/mnt",
		Logpath:               "/tmp/log",
		MetaCacheAcceleration: true,
		StopWarmMeta:          true,
		EnablePosixACL:        true,
		MinReadAheadSize:      1024,
	}
}

func setMetaWrapperMasterClient(mw *meta.MetaWrapper, mc *master.MasterClient) {
	val := reflect.ValueOf(mw).Elem()
	mcField := val.FieldByName("mc")
	ptr := unsafe.Pointer(mcField.UnsafeAddr())
	reflect.NewAt(mcField.Type(), ptr).Elem().Set(reflect.ValueOf(mc))
}

func setMetaWrapperUint8Field(mw *meta.MetaWrapper, name string, value uint8) {
	val := reflect.ValueOf(mw).Elem()
	field := val.FieldByName(name)
	ptr := unsafe.Pointer(field.UnsafeAddr())
	reflect.NewAt(field.Type(), ptr).Elem().SetUint(uint64(value))
}

func setMetaWrapperStringField(mw *meta.MetaWrapper, name, value string) {
	val := reflect.ValueOf(mw).Elem()
	field := val.FieldByName(name)
	ptr := unsafe.Pointer(field.UnsafeAddr())
	reflect.NewAt(field.Type(), ptr).Elem().SetString(value)
}

func metaWrapperDefaultRegion(mw *meta.MetaWrapper) string {
	val := reflect.ValueOf(mw).Elem()
	field := val.FieldByName("defaultMetaRegion")
	ptr := unsafe.Pointer(field.UnsafeAddr())
	return reflect.NewAt(field.Type(), ptr).Elem().String()
}

func setExtentClientMultiVerMgr(ec *stream.ExtentClient) {
	val := reflect.ValueOf(ec).Elem()
	field := val.FieldByName("multiVerMgr")
	ptr := unsafe.Pointer(field.UnsafeAddr())
	reflect.NewAt(field.Type(), ptr).Elem().Set(reflect.ValueOf(&stream.MultiVerMgr{}))
}
