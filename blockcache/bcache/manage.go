// Copyright (C) 2020 Juicefs
// Modified work Copyright 2022 The CubeFS Authors.
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

package bcache

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	PathListSeparator    = ";"
	CacheConfSeparator   = ":"
	SpaceCheckInterval   = 60 * time.Second
	TmpFileCheckInterval = 20 * 60 * time.Second
	FilePerm             = 0o644
	Basedir              = "blocks"
)

type ReadCloser interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type BcacheManager interface {
	cache(_ context.Context, key string, data []byte, direct bool)
	read(_ context.Context, key string, offset uint64, len uint32) (io.ReadCloser, error)
	queryCachePath(_ context.Context, key string, offset uint64, len uint32) (string, error)
	load(_ context.Context, key string) (ReadCloser, error)
	erase(_ context.Context, key string)
	stats(_ context.Context) (int64, int64)
}

func newBcacheManager(conf *bcacheConfig) BcacheManager {
	span, ctx := spanContextPrefix("new-")
	span.Infof("init block cache: %s size:%d GB", conf.CacheDir, conf.BlockSize)
	if conf.CacheDir == "" {
		span.Warn("no cache config,cacheDirs or size is empty!")
		return nil
	}
	// todo cachedir reg match
	cacheDirs := strings.Split(conf.CacheDir, PathListSeparator)
	if len(cacheDirs) == 0 {
		span.Warn("no cache dir config!")
		return nil
	}

	dirSizeMap := make(map[string]int64, len(cacheDirs))
	for _, dir := range cacheDirs {
		result := strings.Split(dir, CacheConfSeparator)
		dirPath := result[0]
		cacheSize, err := strconv.Atoi(result[1])
		if dirPath == "" || err != nil {
			span.Warn("cache dir config error!")
			return nil
		}
		dirSizeMap[dirPath] = int64(cacheSize)
		conf.CacheSize = conf.CacheSize + int64(cacheSize)
	}

	bm := &bcacheManager{
		bstore:     make([]*DiskStore, len(cacheDirs)),
		bcacheKeys: make(map[string]*list.Element),
		lrulist:    list.New(),
		blockSize:  conf.BlockSize,
		pending:    make(chan waitFlush, 1024),
	}
	index := 0
	for cacheDir, cacheSize := range dirSizeMap {
		disk := NewDiskStore(ctx, cacheDir, cacheSize, conf)
		bm.bstore[index] = disk
		go bm.reBuildCacheKeys(ctx, cacheDir, disk)
		index++
	}
	go bm.spaceManager()
	go bm.flush()
	// go bm.scrub()
	return bm
}

type cacheItem struct {
	key  string
	size uint32
}

type keyPair struct {
	key string
	it  *cacheItem
}

// key vid_inode_offset
type waitFlush struct {
	ctx  context.Context
	Key  string
	Data []byte
}

type bcacheManager struct {
	sync.RWMutex
	bcacheKeys map[string]*list.Element
	lrulist    *list.List
	bstore     []*DiskStore
	blockSize  uint32
	pending    chan waitFlush
}

func encryptXOR(data []byte) {
	for index, value := range data {
		data[index] = value ^ byte(0xF)
	}
}

func (bm *bcacheManager) queryCachePath(ctx context.Context, key string, offset uint64, len uint32) (path string, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("GetCache:GetCachePath", err, bgTime, 1)
	}()

	bm.Lock()
	element, ok := bm.bcacheKeys[key]
	bm.Unlock()
	if ok {
		item := element.Value.(*cacheItem)
		path, err := bm.getCachePath(key)
		if err != nil {
			return "", err
		}
		bm.Lock()
		bm.lrulist.MoveToBack(element)
		bm.Unlock()
		getSpan(ctx).Debugf("Cache item found. key=%v offset =%v,len=%v size=%v, path=%v", key, offset, len, item.size, path)
		return path, nil
	}
	getSpan(ctx).Debugf("Cache item not found. key=%v offset =%v,len=%v", key, offset, len)
	return "", os.ErrNotExist
}

func (bm *bcacheManager) getCachePath(key string) (string, error) {
	if len(bm.bstore) == 0 {
		return "", errors.New("no cache dir")
	}
	cachePath := bm.selectDiskKv(key).getPath(key)
	return cachePath, nil
}

func (bm *bcacheManager) cache(ctx context.Context, key string, data []byte, direct bool) {
	span := getSpan(ctx)
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Cache:Write", nil, bgTime, 1)
		stat.StatBandWidth("Cache", uint32(len(data)))
	}()
	span.Debugf("TRACE cache. key(%v)  len(%v) direct(%v)", key, len(data), direct)
	if direct {
		bm.cacheDirect(ctx, key, data)
		return
	}
	select {
	case bm.pending <- waitFlush{ctx: ctx, Key: key, Data: data}:
	default:
		span.Debugf("pending chan is full,skip memory. key =%v,len=%v bytes", key, len(data))
		bm.cacheDirect(ctx, key, data)
	}
}

func (bm *bcacheManager) cacheDirect(ctx context.Context, key string, data []byte) {
	diskKv := bm.selectDiskKv(key)
	if diskKv.flushKey(ctx, key, data) == nil {
		bm.Lock()
		item := &cacheItem{
			key:  key,
			size: uint32(len(data)),
		}
		element := bm.lrulist.PushBack(item)
		bm.bcacheKeys[key] = element
		bm.Unlock()
	}
}

func (bm *bcacheManager) read(ctx context.Context, key string, offset uint64, len uint32) (io.ReadCloser, error) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("GetCache:Read", err, bgTime, 1)
		if err == nil {
			stat.StatBandWidth("GetCache:Read", len)
		}
	}()
	metaBgTime := stat.BeginStat()
	bm.Lock()
	element, ok := bm.bcacheKeys[key]
	bm.Unlock()
	stat.EndStat("GetCache:Read:GetMeta", nil, metaBgTime, 1)

	span := getSpan(ctx)
	span.Debugf("Trace read. ok =%v", ok)
	if ok {
		item := element.Value.(*cacheItem)
		f, err := bm.load(ctx, key)
		if os.IsNotExist(err) {
			bm.Lock()
			delete(bm.bcacheKeys, key)
			bm.Unlock()
			d := bm.selectDiskKv(key)
			atomic.AddInt64(&d.usedSize, -int64(item.size))
			atomic.AddInt64(&d.usedCount, -1)
			return nil, os.ErrNotExist
		}
		if err != nil {
			return nil, err
		}
		defer f.Close()
		size := item.size
		span.Debugf("read. offset =%v,len=%v size=%v", offset, len, size)
		if uint32(offset)+len > size {
			len = size - uint32(offset)
		}
		dataBgTime := stat.BeginStat()
		buf := make([]byte, len)
		n, err := f.ReadAt(buf, int64(offset))
		stat.EndStat("GetCache:Read:ReadData", err, dataBgTime, 1)
		if err != nil {
			return nil, err
		} else {
			// decrypt
			encryptXOR(buf[:n])
			return io.NopCloser(bytes.NewBuffer(buf[:n])), nil
		}
	} else {
		err = os.ErrNotExist
	}
	return nil, err
}

func (bm *bcacheManager) load(ctx context.Context, key string) (ReadCloser, error) {
	if len(bm.bstore) == 0 {
		return nil, errors.New("no cache dir")
	}
	f, err := bm.selectDiskKv(key).load(ctx, key)
	if err != nil {
		return nil, err
	}
	bm.Lock()
	defer bm.Unlock()
	if element, ok := bm.bcacheKeys[key]; ok {
		bm.lrulist.MoveToBack(element)
	}
	return f, err
}

func (bm *bcacheManager) erase(ctx context.Context, key string) {
	if len(bm.bstore) == 0 {
		return
	}
	err := bm.selectDiskKv(key).remove(ctx, key)
	if err == nil {
		bm.Lock()
		defer bm.Unlock()
		if element, ok := bm.bcacheKeys[key]; ok {
			bm.lrulist.Remove(element)
		}
		delete(bm.bcacheKeys, key)
	}
}

func (bm *bcacheManager) stats(ctx context.Context) (int64, int64) {
	var usedCount, usedSize int64
	for _, item := range bm.bstore {
		usedSize += atomic.LoadInt64(&item.usedSize)
		usedCount += atomic.LoadInt64(&item.usedCount)
	}
	return usedCount, usedSize
}

func (bm *bcacheManager) selectDiskKv(key string) *DiskStore {
	return bm.bstore[hashKey(key)%uint32(len(bm.bstore))]
}

func (bm *bcacheManager) spaceManager() {
	ticker := time.NewTicker(SpaceCheckInterval)
	tmpTicker := time.NewTicker(TmpFileCheckInterval)

	defer func() {
		ticker.Stop()
		tmpTicker.Stop()
	}()
	for {
		span, ctx := spanContextPrefix("space-")
		select {
		case <-ticker.C:
			for _, store := range bm.bstore {
				useRatio, files := store.diskUsageRatio(ctx)
				span.Debugf("useRation(%v), files(%v)", useRatio, files)
				if 1-useRatio < store.freeLimit || files > int64(store.limit) {
					bm.freeSpace(ctx, store, 1-useRatio, files)
				}
			}
		case <-tmpTicker.C:
			for _, store := range bm.bstore {
				useRatio, files := store.diskUsageRatio(ctx)
				span.Infof("useRation(%v), files(%v)", useRatio, files)
				bm.deleteTmpFile(ctx, store)
			}
		}
	}
}

// lru
func (bm *bcacheManager) freeSpace(ctx context.Context, store *DiskStore, free float32, files int64) {
	var decreaseSpace int64
	var decreaseCnt int

	if free < store.freeLimit {
		decreaseSpace = int64((store.freeLimit - free) * (float32(store.capacity)))
	}
	if files > int64(store.limit) {
		decreaseCnt = int(files - int64(store.limit))
	}

	span := getSpan(ctx)
	cnt := 0
	for {
		if decreaseCnt <= 0 && decreaseSpace <= 0 {
			break
		}
		// avoid dead loop
		if cnt > 500000 {
			break
		}
		bm.Lock()

		element := bm.lrulist.Front()
		if element == nil {
			bm.Unlock()
			return
		}
		item := element.Value.(*cacheItem)

		if err := store.remove(ctx, item.key); err == nil {
			bm.lrulist.Remove(element)
			delete(bm.bcacheKeys, item.key)
			decreaseSpace -= int64(item.size)
			decreaseCnt--
			cnt++
		}

		bm.Unlock()
		span.Debugf("remove %v from cache", item.key)
	}
}

func (bm *bcacheManager) reBuildCacheKeys(ctx context.Context, dir string, store *DiskStore) {
	span := getSpan(ctx)
	if _, err := os.Stat(dir); err != nil {
		span.Errorf("cache dir %s is not exists", dir)
		return
	}
	span.Debugf("reBuildCacheKeys(%s)", dir)
	c := make(chan keyPair)
	keyPrefix := filepath.Join(dir, Basedir)
	go func() {
		filepath.Walk(dir, bm.walker(ctx, c, keyPrefix, true))
		close(c)
	}()

	for value := range c {
		bm.Lock()
		element := bm.lrulist.PushBack(value.it)
		bm.bcacheKeys[value.key] = element
		bm.Unlock()
		span.Debugf("updateStat(%v)", value.it.size)
		store.updateStat(value.it.size)
	}
}

func (bm *bcacheManager) walker(ctx context.Context, c chan keyPair, prefix string, initial bool) filepath.WalkFunc {
	span := getSpan(ctx)
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			span.Warnf("walk path %v failed %v", path, err)
			return err
		}
		if info.IsDir() || !strings.HasPrefix(path, prefix) {
			return nil
		}
		if strings.HasSuffix(path, ".tmp") && (initial || checkoutTempFileOuttime(path)) {
			os.Remove(path)
			span.Debugf("Remove tmp file %v", path)
			return nil
		}
		_, key := filepath.Split(path)
		size := uint32(info.Size())
		pair := keyPair{
			key: key,
			it: &cacheItem{
				key:  key,
				size: size,
			},
		}
		c <- pair
		return nil
	}
}

func (bm *bcacheManager) flush() {
	for {
		pending := <-bm.pending
		span := getSpan(pending.ctx)
		diskKv := bm.selectDiskKv(pending.Key)
		span.Debugf("flush data, key(%v), dir(%v)", pending.Key, diskKv.dir)
		if diskKv.flushKey(pending.ctx, pending.Key, pending.Data) == nil {
			bm.Lock()
			item := &cacheItem{
				key:  pending.Key,
				size: uint32(len(pending.Data)),
			}
			element := bm.lrulist.PushBack(item)
			bm.bcacheKeys[pending.Key] = element
			bm.Unlock()
		}
	}
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

type DiskStore struct {
	sync.Mutex
	dir       string
	mode      uint32
	capacity  int64
	freeLimit float32
	limit     uint32
	usedSize  int64
	usedCount int64
}

func NewDiskStore(ctx context.Context, dir string, cacheSize int64, config *bcacheConfig) *DiskStore {
	if config.Mode == 0 {
		config.Mode = FilePerm
	}
	if config.FreeRatio <= 0 {
		config.FreeRatio = 0.15
	}

	if config.Limit <= 0 {
		config.Limit = 50000000
	}

	if config.Limit > 50000000 {
		config.Limit = 50000000
	}
	c := &DiskStore{
		dir:       dir,
		mode:      config.Mode,
		capacity:  cacheSize,
		freeLimit: config.FreeRatio,
		limit:     config.Limit,
	}
	getSpan(ctx).Debugf("ignored method DiskStore.scrub at %p", c.scrub) // TODO: ignored
	c.checkBuildCacheDir(dir)
	return c
}

func (d *DiskStore) checkBuildCacheDir(dir string) {
	mode := os.FileMode(d.mode)
	if st, err := os.Stat(dir); os.IsNotExist(err) {
		if parent := filepath.Dir(dir); parent != dir {
			d.checkBuildCacheDir(parent)
		}
		os.Mkdir(dir, mode)
	} else if err != nil && st.Mode() != mode {
		os.Chmod(dir, mode)
	}
}

func (d *DiskStore) flushKey(ctx context.Context, key string, data []byte) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("Cache:Write:FlushData", err, bgTime, 1)
	}()
	cachePath := d.buildCachePath(key, d.dir)
	info, err := os.Stat(cachePath)
	// if already cached
	if err == nil && info.Size() > 0 {
		return nil
	}
	span := getSpan(ctx)
	span.Debugf("TRACE BCacheService flushKey Enter. key(%v) cachePath(%v)", key, cachePath)
	d.checkBuildCacheDir(filepath.Dir(cachePath))
	tmp := cachePath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(d.mode))
	defer os.Remove(tmp)
	if err != nil {
		span.Warnf("Create block tmp file:%s err:%s!", tmp, err)
		return err
	}
	// encrypt
	encryptXOR(data)
	_, err = f.Write(data)
	if err != nil {
		f.Close()
		span.Errorf("Write tmp failed: file %s err %s!", tmp, err)
		return err
	}
	err = f.Close()
	if err != nil {
		span.Errorf("Close tmp failed: file:%s err:%s!", tmp, err)
		return err
	}
	//info, err := os.Stat(cachePath)
	////if already cached
	//if !os.IsNotExist(err) {
	//	atomic.AddInt64(&d.usedSize, -(info.Size()))
	//	atomic.AddInt64(&d.usedCount, -1)
	//	os.Remove(cachePath)
	//}
	err = os.Rename(tmp, cachePath)
	if err != nil {
		span.Errorf("Rename block tmp file:%s err:%s!", tmp, err)
		return err
	}
	atomic.AddInt64(&d.usedSize, int64(len(data)))
	atomic.AddInt64(&d.usedCount, 1)
	span.Debugf("TRACE BCacheService flushKey Exit. key(%v) cachePath(%v)", key, cachePath)
	return nil
}

func (d *DiskStore) load(ctx context.Context, key string) (ReadCloser, error) {
	span := getSpan(ctx)
	cachePath := d.buildCachePath(key, d.dir)
	span.Debugf("TRACE BCacheService load Enter. key(%v) cachePath(%v)", key, cachePath)
	//if _, err := os.Stat(cachePath); err != nil {
	//	return nil, errors.NewError(os.ErrNotExist)
	//}
	f, err := os.OpenFile(cachePath, os.O_RDONLY, os.FileMode(d.mode))
	span.Debugf("TRACE BCacheService load Exit. err(%v)", err)
	return f, err
}

func (d *DiskStore) remove(ctx context.Context, key string) (err error) {
	var size int64
	cachePath := d.buildCachePath(key, d.dir)
	getSpan(ctx).Debugf("remove. cachePath(%v)", cachePath)
	if info, err := os.Stat(cachePath); err == nil {
		size = info.Size()
		if err = os.Remove(cachePath); err == nil {
			atomic.AddInt64(&d.usedSize, -size)
			atomic.AddInt64(&d.usedCount, -1)
		}
	}
	return err
}

func (d *DiskStore) buildCachePath(key string, dir string) string {
	inodeId, err := strconv.ParseInt(strings.Split(key, "_")[1], 10, 64)
	if err != nil {
		return fmt.Sprintf("%s/blocks/%d/%d/%s", dir, hashKey(key)&0xFFF%512, hashKey(key)%512, key)
	}
	return fmt.Sprintf("%s/blocks/%d/%d/%s", dir, hashKey(key)&0xFFF%512, inodeId%512, key)
}

func (d *DiskStore) diskUsageRatio(ctx context.Context) (float32, int64) {
	getSpan(ctx).Debugf("usedSize(%v), usedCount(%v)", atomic.LoadInt64(&d.usedSize), atomic.LoadInt64(&d.usedCount))
	if atomic.LoadInt64(&d.usedSize) < 0 || atomic.LoadInt64(&d.usedCount) < 0 {
		return 0, 0
	}
	return float32(atomic.LoadInt64(&d.usedSize)) / float32(d.capacity), atomic.LoadInt64(&d.usedCount)
}

func (d *DiskStore) scrub(key string, md5Sum string) error {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	cachePath := d.buildCachePath(key, d.dir)
	f, err := os.Open(cachePath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	h := md5.New()
	_, err = io.Copy(h, r)
	if err != nil {
		return err
	}
	if md5Sum != hex.EncodeToString(h.Sum(nil)) {
		return errors.New("scrub error")
	}
	return nil
}

func (d *DiskStore) updateStat(size uint32) {
	atomic.AddInt64(&d.usedSize, int64(size))
	atomic.AddInt64(&d.usedCount, 1)
}

func (d *DiskStore) getPath(key string) string {
	cachePath := d.buildCachePath(key, d.dir)
	return cachePath
}

func (bm *bcacheManager) deleteTmpFile(ctx context.Context, store *DiskStore) {
	span := getSpan(ctx)
	if _, err := os.Stat(store.dir); err != nil {
		span.Errorf("cache dir %s is not exists", store.dir)
		return
	}
	span.Debugf("clear tmp files in %v", store.dir)
	c := make(chan keyPair)
	keyPrefix := filepath.Join(store.dir, Basedir)
	span.Debugf("keyPrefix %v", keyPrefix)
	go func() {
		filepath.Walk(store.dir, bm.walker(ctx, c, keyPrefix, false))
		close(c)
	}()
	// consume chan
	for range c {
	}
	span.Debugf("clear tmp files end %v", store.dir)
}

func checkoutTempFileOuttime(file string) bool {
	finfo, err := os.Stat(file)
	if err != nil {
		return false
	}
	stat_t := finfo.Sys().(*syscall.Stat_t)
	now := time.Now()
	return now.Sub(timespecToTime(stat_t.Ctim)).Seconds() > 60*60 // 1 hour
}

func timespecToTime(ts syscall.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}
