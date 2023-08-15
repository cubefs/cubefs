// Copyright 2023 The CubeFS Authors.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/retry"
	"github.com/rs/xid"
)

const (
	defaultConsumeBatchCnt = 1000
	defaultConcurrency     = 3
	minGranularityMin      = 5
	maxGranularityMin      = 60
	dateStringLength       = 10

	minConsumeWaitTime = 1000 * time.Millisecond
)

var loggingBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, util.BlockSize)
	},
}

type VolumeGetter interface {
	getVol(name string) (*Volume, error)
}

type LoggingManager interface {
	Start()
	Send(ctx context.Context, data string) error
	Close() error
}

type LoggingMgr struct {
	topic         string
	producer      *kafka.Producer
	consumerGroup sarama.ConsumerGroup
	writers       map[string]LoggingWriter

	mtx    sync.RWMutex
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	VolumeGetter
	LoggingConfig
}

type LoggingConfig struct {
	EnableConsume     bool  `json:"enable_consume"`
	ConsumeBatchCnt   int   `json:"consume_batch_cnt"`
	Concurrency       int   `json:"concurrency"`
	GranularityMin    int   `json:"granularity_min"`
	ConsumeIntervalMs int64 `json:"consume_interval_ms"`

	kafka.ProducerCfg
}

func (cfg *LoggingConfig) checkAndFix() error {
	if cfg.ConsumeBatchCnt <= 0 {
		cfg.ConsumeBatchCnt = defaultConsumeBatchCnt
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.GranularityMin < minGranularityMin {
		cfg.GranularityMin = minGranularityMin
	}
	if cfg.GranularityMin > maxGranularityMin {
		cfg.GranularityMin = maxGranularityMin
	}

	return nil
}

func NewLoggingManager(getter VolumeGetter, conf LoggingConfig) (LoggingManager, error) {
	if err := conf.checkAndFix(); err != nil {
		return nil, err
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Metadata.RefreshFrequency = 120 * time.Second
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	groupID := conf.Topic
	consumerGroup, err := sarama.NewConsumerGroup(conf.BrokerList, groupID, cfg)
	if err != nil {
		return nil, err
	}
	producer, err := kafka.NewProducer(&conf.ProducerCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &LoggingMgr{
		topic:         conf.Topic,
		producer:      producer,
		consumerGroup: consumerGroup,
		writers:       make(map[string]LoggingWriter),
		ctx:           ctx,
		cancel:        cancel,
		VolumeGetter:  getter,
		LoggingConfig: conf,
	}, nil
}

func (l *LoggingMgr) Start() {
	if l.EnableConsume {
		go l.startConsumer()
		go l.checkWriters()
	}
}

func (l *LoggingMgr) Close() error {
	l.cancel()
	l.once.Do(func() {
		l.producer.Close()
		l.consumerGroup.Close()
	})
	for _, w := range l.writers {
		_ = w.Close()
	}
	return nil
}

func (l *LoggingMgr) Send(ctx context.Context, data string) (err error) {
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	defer func() {
		span.AppendTrackLog("lsender", start, err)
	}()

	msg, err := l.parseLine(data)
	if err != nil {
		span.Errorf("parse [%s] failed: %v", data, err)
		return
	}
	if len(msg) > 0 {
		if err = l.producer.SendMessage(l.topic, msg); err != nil {
			span.Errorf("send [%s] to [%s] failed: %v", string(msg), l.topic, err)
		}
	}

	return
}

func (l *LoggingMgr) parseLine(line string) (data []byte, err error) {
	entry, err := auditlog.ParseReqlog(line)
	if err != nil || entry.Bucket() == "" {
		return
	}

	vol, err := l.getVol(entry.Bucket())
	if err != nil {
		return
	}
	logging, err := vol.metaLoader.loadLogging()
	if err != nil || logging == nil || logging.LoggingEnabled == nil {
		return
	}

	bucket := logging.LoggingEnabled.TargetBucket
	prefix := logging.LoggingEnabled.TargetPrefix
	data, err = json.Marshal(makeLoggingMessage(bucket, prefix, entry))

	return
}

func (l *LoggingMgr) startConsumer() {
	for {
		if err := l.consumerGroup.Consume(l.ctx, []string{l.topic}, l); err != nil {
			log.LogCriticalf("consume error: %v", err)
			time.Sleep(time.Minute)
			continue
		}
		log.LogInfof("partition rebalanced for topic: %s", l.topic)
		if l.ctx.Err() != nil {
			return
		}
	}
}

func (l *LoggingMgr) checkWriters() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			var deletes []string
			for key, writer := range l.getAllWriters() {
				if len(key) > dateStringLength && writer != nil {
					dt, err := time.Parse("2006-01-02", key[len(key)-dateStringLength:])
					if err != nil {
						log.LogErrorf("invalid writer key: %s", key)
						continue
					}
					now := time.Now().UTC()
					if now.Sub(dt) > 24*time.Hour && now.Sub(writer.ModifyTime()) > time.Hour {
						deletes = append(deletes, key)
					}
					log.LogInfof("check writer key: %s, date: %v, now: %v, mtime: %v", key, dt, now, writer.ModifyTime())
				} else {
					log.LogWarnf("invalid writer key: %s", key)
				}
			}
			if len(deletes) > 0 {
				l.deleteWriters(deletes)
			}
		}
	}
}

func (l *LoggingMgr) Setup(sess sarama.ConsumerGroupSession) error {
	partitions, ok := sess.Claims()[l.topic]
	if !ok {
		return fmt.Errorf("not found topic %s", l.topic)
	}
	log.LogInfof("kafka consumer group setup, partitions: %v", partitions)
	return nil
}

func (l *LoggingMgr) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (l *LoggingMgr) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var msgs []*sarama.ConsumerMessage

	consumeWait := time.Millisecond / 2 * time.Duration(l.ConsumeBatchCnt)
	if consumeWait < minConsumeWaitTime {
		consumeWait = minConsumeWaitTime
	}
	ticker := time.NewTicker(consumeWait)
	defer ticker.Stop()

	for {
		var msg *sarama.ConsumerMessage
		select {
		case msg = <-claim.Messages():
			if msg == nil {
				continue
			}
			msgs = append(msgs, msg)
		case <-ticker.C:
			if len(msgs) <= 0 || (msg != nil && len(msgs) < l.ConsumeBatchCnt) {
				continue
			}
			l.handleMessages(sess, msgs)
			if l.ConsumeIntervalMs > 0 {
				time.Sleep(time.Duration(l.ConsumeIntervalMs) * time.Millisecond)
			}
			msgs = msgs[:0]
			ticker.Reset(consumeWait)
		case <-l.ctx.Done():
			return nil
		}
	}
}

func (l *LoggingMgr) getWriter(name string) LoggingWriter {
	l.mtx.RLock()
	defer l.mtx.RUnlock()
	return l.writers[name]
}

func (l *LoggingMgr) getOrSetWriter(key string) (writer LoggingWriter, err error) {
	keys := strings.SplitN(key, "/", 2)
	if len(keys) != 2 {
		log.LogWarnf("invalid key: %s", key)
		return
	}
	bucket, dir := keys[0], keys[1]
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if writer = l.writers[key]; writer == nil {
		var vol *Volume
		if err = retry.InsistWithDefaultExponential(l.ctx, func() error {
			if vol, err = l.getVol(bucket); err != nil {
				log.LogErrorf("volume %s get failed: %v", bucket, err)
			}
			if err == NoSuchBucket {
				return nil
			}
			return err
		}); err != nil || vol == nil {
			return
		}
		if writer, err = newLoggingWriter(l.ctx, vol, dir+"/"); err == nil {
			l.writers[key] = writer
		}
	}

	return
}

func (l *LoggingMgr) getAllWriters() map[string]LoggingWriter {
	l.mtx.RLock()
	defer l.mtx.RUnlock()
	a := make(map[string]LoggingWriter, len(l.writers))
	for k, v := range l.writers {
		a[k] = v
	}
	return a
}

func (l *LoggingMgr) deleteWriters(keys []string) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	for _, k := range keys {
		if writer := l.writers[k]; writer != nil {
			if err := writer.Close(); err != nil {
				log.LogWarnf("writer close failed: %v", err)
			}
		}
		delete(l.writers, k)
	}
}

func (l *LoggingMgr) handleMessages(sess sarama.ConsumerGroupSession, msgs []*sarama.ConsumerMessage) {
	dataset := make(map[string][]byte)
	for _, msg := range msgs {
		if msg != nil {
			lmsg := new(LoggingMessage)
			if err := json.Unmarshal(msg.Value, lmsg); err != nil {
				log.LogErrorf("unmarshal (%s) failed: %v", string(msg.Value), err)
				continue
			}
			name, data, err := makeLoggingNameData(lmsg, l.GranularityMin)
			if err != nil {
				log.LogErrorf("make (%+v) to logging data failed: %v", lmsg, err)
				continue
			}
			dataset[name] = append(dataset[name], data...)
		}
	}

	for name, data := range dataset {
		var writer LoggingWriter
		key := filepath.Dir(name)
		if writer = l.getWriter(key); writer == nil {
			var err error
			if writer, err = l.getOrSetWriter(key); err != nil {
				return
			}
		}
		if writer != nil {
			writer.Write(filepath.Base(name), data)
		}
	}

	sess.MarkMessage(msgs[len(msgs)-1], "")
}

type LoggingWriter interface {
	ModifyTime() time.Time
	Write(name string, data []byte) error
	Close() error
}

type loggingWrite struct {
	parentIno  uint64
	dir        string
	vol        *Volume
	ctx        context.Context
	files      map[string]*proto.InodeInfo
	modifyTime time.Time

	once   sync.Once
	closed chan struct{}
	sync.RWMutex
}

func newLoggingWriter(ctx context.Context, vol *Volume, dir string) (LoggingWriter, error) {
	w := &loggingWrite{
		vol:    vol,
		dir:    dir,
		ctx:    ctx,
		files:  make(map[string]*proto.InodeInfo),
		closed: make(chan struct{}),
	}
	if err := w.makeParentDir(); err != nil {
		return nil, err
	}

	go w.checkFiles()

	return w, nil
}

func (w *loggingWrite) ModifyTime() time.Time {
	return w.modifyTime
}

func (w *loggingWrite) Write(name string, data []byte) error {
	return retry.InsistWithDefaultExponential(w.ctx, func() error {
		w.modifyTime = time.Now().UTC()
		info, err := w.openStream(name)
		if err != nil {
			return err
		}
		w.Lock()
		defer w.Unlock()

		offset := info.Size
		wn, err := w.write(info.Inode, int(offset), bytes.NewReader(data))
		if err != nil {
			return err
		}
		if err = w.vol.ec.Flush(info.Inode); err != nil {
			log.LogErrorf("flush file %d failed: %v", info.Inode, err)
			if err == syscall.ENOENT {
				log.LogWarnf("file %v has been deleted elsewhere, recreate another", info.Inode)
				delete(w.files, name)
				_ = w.vol.ec.CloseStream(info.Inode)
			}
			return err
		}
		info.Size += uint64(wn)
		info.ModifyTime = time.Now()
		log.LogInfof("write file inode: %d, size: %d/%d, write: %d, name: %s", info.Inode, offset, info.Size, wn, w.dir+name)

		return nil
	})
}

func (w *loggingWrite) Close() error {
	w.once.Do(func() {
		close(w.closed)
	})
	return nil
}

func (w *loggingWrite) checkFiles() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.closed:
			return
		case <-ticker.C:
			var deletes []string
			for key, info := range w.getAllFiles() {
				if info != nil && time.Since(info.ModifyTime) > time.Hour {
					deletes = append(deletes, key)
				}
				log.LogInfof("check file key: %s, now: %v, mtime: %v", key, time.Now(), info.ModifyTime)
			}
			if len(deletes) > 0 {
				w.deleteFiles(deletes)
			}
		}
	}
}

func (w *loggingWrite) getAllFiles() map[string]*proto.InodeInfo {
	w.RLock()
	defer w.RUnlock()
	a := make(map[string]*proto.InodeInfo, len(w.files))
	for k, v := range w.files {
		a[k] = v
	}
	return a
}

func (w *loggingWrite) deleteFiles(keys []string) {
	w.Lock()
	defer w.Unlock()
	for _, key := range keys {
		if info := w.files[key]; info != nil {
			if err := w.vol.ec.CloseStream(info.Inode); err != nil {
				log.LogErrorf("stream %d close failed: %v", info.Inode, err)
			}
		}
		delete(w.files, key)
	}
}

func (w *loggingWrite) makeParentDir() error {
	return retry.InsistWithDefaultExponential(w.ctx, func() error {
		inode, err := w.vol.recursiveMakeDirectory(w.dir)
		if err != nil {
			log.LogErrorf("make dir %s failed: %v", w.dir, err)
			return err
		}
		w.parentIno = inode
		return nil
	})
}

func (w *loggingWrite) getInodeInfo(name string) *proto.InodeInfo {
	w.RLock()
	defer w.RUnlock()
	return w.files[name]
}

func (w *loggingWrite) openStream(key string) (info *proto.InodeInfo, err error) {
	if info = w.getInodeInfo(key); info == nil {
		w.Lock()
		defer w.Unlock()
		if info = w.files[key]; info == nil {
			info, err = w.vol.mw.InodeCreate_ll(w.parentIno, DefaultFileMode, 0, 0, nil, nil)
			if err != nil {
				log.LogErrorf("create sub inode of %d failed: %v", w.parentIno, err)
				return
			}
			name := fmt.Sprintf("%s-%s", key, xid.New().String())
			if err = w.vol.applyInodeToDEntry(w.parentIno, name, info.Inode); err != nil {
				log.LogErrorf("apply %d/%s dentry failed: %v", info.Inode, name, err)
				_, _ = w.vol.mw.InodeUnlink_ll(info.Inode)
				return
			}
			w.files[key] = info
		}
	}
	if err = w.vol.ec.OpenStream(info.Inode); err != nil {
		log.LogErrorf("open stream failed: %v", err)
	}

	return
}

func (w *loggingWrite) write(inode uint64, offset int, reader io.Reader) (write int, err error) {
	buf := loggingBufPool.Get().([]byte)
	defer loggingBufPool.Put(buf)

	checkFunc := func() error {
		if !w.vol.mw.EnableQuota {
			return nil
		}
		if ok := w.vol.ec.UidIsLimited(0); ok {
			return syscall.ENOSPC
		}
		if w.vol.mw.IsQuotaLimitedById(inode, true, false) {
			return syscall.ENOSPC
		}
		return nil
	}
	var rn, wn int
	for {
		rn, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			log.LogErrorf("read data failed: %v", err)
			break
		}
		if rn > 0 {
			wn, err = w.vol.ec.Write(inode, offset, buf[:rn], proto.FlagsAppend, checkFunc)
			if err != nil {
				log.LogErrorf("stream write off(%d) of ino(%d) failed: %v", offset, inode, err)
				break
			}
			write += wn
			offset += wn
		}
		if err == io.EOF {
			err = nil
			break
		}
	}

	return
}
