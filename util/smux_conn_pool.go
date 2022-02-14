// Copyright 2018 The Chubao Authors.
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

package util

import (
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"io"
	"net"
	"smux"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	DefaultSmuxPortShift = 500
)

const (
	defaultCreateInterval = int64(time.Microsecond * 200)
)

var (
	ErrTooMuchSmuxStreams = errors.New("too much smux streams")
)

// addr = ip:port
// afterShift = ip:(port+shift)
func ShiftAddrPort(addr string, shift int) (afterShift string) {
	pars := strings.Split(addr, ":")
	if len(pars) != 2 {
		return
	}
	ip, port := pars[0], pars[1]
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return
	}
	afterShift = fmt.Sprintf("%s:%d", ip, portNum+shift)
	return
}

//filter smux accept error
func FilterSmuxAcceptError(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == io.EOF.Error() {
		return nil
	}
	if operr, ok := err.(*net.OpError); ok {
		if operr.Err == syscall.ECONNRESET {
			return nil
		}
	}
	return err
}

const (
	streamPreAlloc = 1
	connPreAlloc   = 1
)

type SmuxConnPoolConfig struct {
	*smux.Config
	TotalStreams      int
	StreamsPerConn    int
	ConnsPerAddr      int
	PoolCapacity      int
	DialTimeout       time.Duration
	StreamIdleTimeout int64
}

func DefaultSmuxConnPoolConfig() *SmuxConnPoolConfig {
	return &SmuxConnPoolConfig{
		Config:            DefaultSmuxConfig(),
		TotalStreams:      1000000,
		StreamsPerConn:    1,
		ConnsPerAddr:      16,
		PoolCapacity:      64,
		DialTimeout:       time.Second * 10,
		StreamIdleTimeout: int64(time.Second * 60),
	}
}

func VerifySmuxPoolConfig(cfg *SmuxConnPoolConfig) error {
	if err := smux.VerifyConfig(cfg.Config); err != nil {
		return err
	}
	if cfg.ConnsPerAddr <= 0 {
		return errors.New("cfg.ConnsPerAddr must be larger than 0")
	}
	if cfg.PoolCapacity <= 0 {
		return errors.New("cfg.PoolCapacity must be larger than 0")
	}
	if cfg.StreamsPerConn <= 0 {
		return errors.New("cfg.StreamsPerConn must be larger than 0")
	}
	if cfg.StreamIdleTimeout < int64(10*time.Millisecond) {
		return errors.New("cfg.StreamIdleTimeout too small, must be larger than 10ms")
	}
	if cfg.TotalStreams <= 0 {
		return errors.New("cfg.TotalStreams must be larger than 0")
	}
	return nil
}

func DefaultSmuxConfig() *smux.Config {
	return smux.DefaultConfig()
}

var gConfig = DefaultSmuxConnPoolConfig()

type SmuxConnPoolStat struct {
	TotalStreams         int                      `json:"totalStreams"`
	TotalStreamsReported int                      `json:"totalStreamsInflight"`
	Pools                map[string]*SmuxPoolStat `json:"pools"`
	TotalSessions        int                      `json:"totalSessions"`
}

// token bucket limit
type simpleTokenBucket struct {
	bucket  int64
	notify  chan struct{}
	blocked bool
}

func newSimpleTokenBucket(n int64, blocked bool) *simpleTokenBucket {
	return &simpleTokenBucket{
		bucket:  n,
		notify:  make(chan struct{}, 1),
		blocked: blocked,
	}
}

func (b *simpleTokenBucket) consumeTokens(n int) bool {
	if atomic.AddInt64(&b.bucket, int64(-n)) < 0 {
		if b.blocked {
			<-b.notify
		} else {
			atomic.AddInt64(&b.bucket, int64(n))
			return false
		}
	}
	return true
}

func (b *simpleTokenBucket) returnTokens(n int) {
	if atomic.AddInt64(&b.bucket, int64(n)) > 0 {
		if b.blocked {
			select {
			case b.notify <- struct{}{}:
			default:
			}
		}
	}
}

type SmuxConnectPool struct {
	sync.RWMutex
	streamBucket *simpleTokenBucket
	cfg          *SmuxConnPoolConfig
	pools        map[string]*SmuxPool
	closeCh      chan struct{}
	closeOnce    sync.Once
}

func NewSmuxConnectPool(cfg *SmuxConnPoolConfig) (cp *SmuxConnectPool) {
	if cfg == nil {
		cfg = gConfig
	}
	cp = &SmuxConnectPool{
		streamBucket: newSimpleTokenBucket(int64(cfg.TotalStreams), false),
		cfg:          cfg,
		pools:        make(map[string]*SmuxPool),
		closeCh:      make(chan struct{}),
		closeOnce:    sync.Once{},
	}
	go cp.autoRelease()

	return cp
}

func (cp *SmuxConnectPool) GetConnect(targetAddr string) (c *smux.Stream, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool, ok = cp.pools[targetAddr]
		if !ok {
			pool = NewSmuxPool(cp.cfg, targetAddr, cp.streamBucket)
			cp.pools[targetAddr] = pool
		}
		cp.Unlock()
	}
	return pool.GetConnect()
}

func (cp *SmuxConnectPool) PutConnect(stream *smux.Stream, forceClose bool) {
	if stream == nil {
		return
	}
	select {
	case <-cp.closeCh:
		return
	default:
	}
	addr := stream.RemoteAddr().String()
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		return
	}
	if forceClose {
		stream.Close()
		pool.MarkClosed(stream)
		return
	}
	pool.PutStreamObjectToPool(&streamObject{stream: stream, idle: time.Now().UnixNano()})
	return
}

func (cp *SmuxConnectPool) autoRelease() {
	var timer = time.NewTimer(time.Duration(cp.cfg.StreamIdleTimeout))
	for {
		select {
		case <-cp.closeCh:
			timer.Stop()
			return
		case <-timer.C:
		}
		pools := make([]*SmuxPool, 0)
		cp.RLock()
		for _, pool := range cp.pools {
			pools = append(pools, pool)
		}
		cp.RUnlock()
		for _, pool := range pools {
			pool.autoRelease()
		}
		timer.Reset(time.Duration(cp.cfg.StreamIdleTimeout))
	}
}

func (cp *SmuxConnectPool) releaseAll() {
	pools := make([]*SmuxPool, 0)
	cp.RLock()
	for _, pool := range cp.pools {
		pools = append(pools, pool)
	}
	cp.RUnlock()
	for _, pool := range pools {
		pool.ReleaseAll()
	}
}

func (cp *SmuxConnectPool) Close() {
	cp.closeOnce.Do(func() {
		close(cp.closeCh)
		cp.releaseAll()
	})
}

func (cp *SmuxConnectPool) GetStat() *SmuxConnPoolStat {
	stat := &SmuxConnPoolStat{
		TotalStreams:         0,
		TotalStreamsReported: 0,
		Pools:                make(map[string]*SmuxPoolStat),
		TotalSessions:        0,
	}
	cp.RLock()
	for remote, pool := range cp.pools {
		stat.Pools[remote] = pool.GetStat()
	}
	cp.RUnlock()
	for _, poolStat := range stat.Pools {
		stat.TotalSessions += poolStat.TotalSessions
		stat.TotalStreams += poolStat.InflightStreams
		stat.TotalStreamsReported += poolStat.InflightStreamsReported
	}
	return stat
}

type createSessCall struct {
	idle   int64
	notify chan struct{}
	sess   *smux.Session
	err    error
}

type streamObject struct {
	stream *smux.Stream
	idle   int64
}

type SmuxPool struct {
	target          string
	sessionsLock    sync.RWMutex
	sessionsIter    int64
	sessions        []*smux.Session
	cfg             *SmuxConnPoolConfig
	objects         chan *streamObject
	inflightStreams int64
	createSessCall  *createSessCall
	streamBucket    *simpleTokenBucket
}

type SmuxPoolStat struct {
	Addr                    string         `json:"addr"`
	InflightStreams         int            `json:"inflightStreams"`
	InflightStreamsReported int            `json:"inflightStreamReported"`
	TotalSessions           int            `json:"totalSessions"`
	StreamsPerSession       map[string]int `json:"streamsPerSession"`
}

func NewSmuxPool(cfg *SmuxConnPoolConfig, target string, streamBucket *simpleTokenBucket) (p *SmuxPool) {
	if cfg == nil {
		cfg = gConfig
	}
	p = &SmuxPool{
		target:       target,
		sessions:     make([]*smux.Session, 0, cfg.ConnsPerAddr),
		cfg:          cfg,
		streamBucket: streamBucket,
		objects:      make(chan *streamObject, cfg.PoolCapacity),
	}
	p.initSessions()
	p.initStreams()
	return p
}

func (p *SmuxPool) initSessions() {
	p.sessionsLock.Lock()
	defer p.sessionsLock.Unlock()
	for i := 0; i < connPreAlloc; i++ {
		conn, err := net.DialTimeout("tcp", p.target, p.cfg.DialTimeout)
		if err != nil {
			continue
		}
		sess, err := smux.Client(conn, p.cfg.Config)
		if err != nil {
			conn.Close()
			continue
		}
		p.sessions = append(p.sessions, sess)
	}
}

func (p *SmuxPool) initStreams() {
	for i := 0; i < streamPreAlloc; i++ {
		stream, err := p.NewStream()
		if err == nil {
			p.PutStreamObjectToPool(&streamObject{
				stream: stream,
				idle:   time.Now().UnixNano(),
			})
		}
	}
}

func (p *SmuxPool) callCreate() (createCall *createSessCall) {
	createCall = p.loadCreateCall()
	if createCall == nil {
		goto tryCreateNewSess
	}
	select {
	case <-createCall.notify:
		if time.Now().UnixNano()-createCall.idle > defaultCreateInterval {
			goto tryCreateNewSess
		} else {
			return
		}
	default:
	}
tryCreateNewSess:
	prev := createCall
	createCall = &createSessCall{
		idle:   time.Now().UnixNano(),
		notify: make(chan struct{}),
	}
	if p.casCreateCall(prev, createCall) {
		go p.handleCreateCall(createCall)
		return createCall
	} else {
		return p.loadCreateCall()
	}
}

func (p *SmuxPool) autoRelease() {
	poolLen := len(p.objects)
getFromPool:
	for i := 0; i < poolLen; i++ {
		select {
		case obj := <-p.objects:
			if streamClosed(obj.stream) {
				p.MarkClosed(obj.stream)
			} else if time.Now().UnixNano()-obj.idle > p.cfg.StreamIdleTimeout {
				obj.stream.Close()
				p.MarkClosed(obj.stream)
			} else {
				p.PutStreamObjectToPool(obj)
			}
		default:
			break getFromPool
		}
	}
	p.sessionsLock.Lock()
	defer p.sessionsLock.Unlock()
	sessionsLen := len(p.sessions)
	hole := 0
	for i := 0; i+hole < sessionsLen; {
		o := p.sessions[i]
		if o.IsClosed() {
			p.sessions[i] = nil
			hole++
		} else if o.NumStreams() == 0 {
			o.Close()
			p.sessions[i] = nil
			hole++
		} else {
			i++
		}
		if hole > 0 && i+hole < sessionsLen {
			p.sessions[i] = p.sessions[i+hole]
		}
	}
	if hole > 0 {
		p.sessions = p.sessions[:sessionsLen-hole]
	}
}

func streamClosed(stream *smux.Stream) bool {
	select {
	case <-stream.GetDieCh():
		return true
	default:
		return false
	}
}

func (p *SmuxPool) canUse(sess *smux.Session) bool {
	if sess == nil || sess.IsClosed() {
		return false
	}
	streamNum := sess.NumStreams()
	if streamNum > 0 {
		if streamNum < p.cfg.StreamsPerConn {
			return true
		}
		maxStreams := p.cfg.StreamsPerConn * p.cfg.ConnsPerAddr
		inflight := p.inflightStreamNum()
		if inflight >= maxStreams {
			//oversold
			return streamNum <= ((inflight / p.cfg.ConnsPerAddr) + 1)
		} else {
			return false
		}
	} else {
		return true
	}
}

func (p *SmuxPool) ReleaseAll() {
	p.sessionsLock.Lock()
	defer p.sessionsLock.Unlock()
	sessionsLen := len(p.sessions)
	for i := 0; i < sessionsLen; i++ {
		o := p.sessions[i]
		if o != nil {
			o.Close()
			p.sessions[i] = nil
		}
	}
	p.sessions = p.sessions[:0]
}

func (p *SmuxPool) getAvailSess() (sess *smux.Session) {
	//every time start from different pos
	iter := atomic.AddInt64(&p.sessionsIter, 1) - 1
	p.sessionsLock.RLock()
	sessionsLen := len(p.sessions)
	for i := 0; i < sessionsLen; i++ {
		o := p.sessions[(int64(i)+iter)%int64(sessionsLen)]
		if p.canUse(o) {
			sess = o
			break
		}
	}
	p.sessionsLock.RUnlock()
	return
}

func (p *SmuxPool) insertSession(sess *smux.Session) {
	p.sessionsLock.Lock()
	//replace
	for i, o := range p.sessions {
		if o == nil || o.IsClosed() {
			p.sessions[i] = sess
			p.sessionsLock.Unlock()
			return
		}
	}
	//or append
	p.sessions = append(p.sessions, sess)
	p.sessionsLock.Unlock()
}

func (p *SmuxPool) GetConnect() (*smux.Stream, error) {
	poolLen := len(p.objects)
getFromPool:
	for i := 0; i < poolLen; i++ {
		select {
		case obj := <-p.objects:
			if obj != nil {
				select {
				case <-obj.stream.GetDieCh():
					p.MarkClosed(obj.stream)
					continue getFromPool
				default:
					return obj.stream, nil
				}
			}
		default:
			break getFromPool
		}
	}
	return p.NewStream()
}

func (p *SmuxPool) NewStream() (stream *smux.Stream, err error) {
	sess := p.getAvailSess()
	if sess != nil {
		stream, err = p.openStream(sess)
		if err != nil {
			goto createNewSession
		} else {
			return
		}
	}
createNewSession:
	call := p.callCreate()
	<-call.notify
	if call.err != nil {
		return nil, call.err
	} else {
		return p.openStream(call.sess)
	}
}

func (p *SmuxPool) MarkClosed(s *smux.Stream) {
	s.Close()
	p.addInflightStream(-1)
	p.streamBucket.returnTokens(1)
}

func (p *SmuxPool) addInflightStream(n int) int {
	return int(atomic.AddInt64(&p.inflightStreams, int64(n)))
}

func (p *SmuxPool) inflightStreamNum() int {
	return int(atomic.LoadInt64(&p.inflightStreams))
}

func (p *SmuxPool) loadCreateCall() *createSessCall {
	return (*createSessCall)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&p.createSessCall))))
}

func (p *SmuxPool) casCreateCall(prev *createSessCall, new *createSessCall) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.createSessCall)),
		unsafe.Pointer(prev), unsafe.Pointer(new))
}

func (p *SmuxPool) handleCreateCall(call *createSessCall) {
	var conn net.Conn
	defer close(call.notify)
	conn, call.err = net.DialTimeout("tcp", p.target, p.cfg.DialTimeout)
	if call.err != nil {
		return
	}
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	call.sess, call.err = smux.Client(conn, p.cfg.Config)
	if call.err != nil {
		c.Close()
		return
	}
	p.insertSession(call.sess)
	return
}

func (p *SmuxPool) openStream(sess *smux.Session) (stream *smux.Stream, err error) {
	if !p.streamBucket.consumeTokens(1) {
		return nil, ErrTooMuchSmuxStreams
	}
	stream, err = sess.OpenStream()
	if err == nil {
		p.addInflightStream(1)
	} else {
		p.streamBucket.returnTokens(1)
	}
	return
}

func (p *SmuxPool) PutStreamObjectToPool(obj *streamObject) {
	if streamClosed(obj.stream) {
		p.MarkClosed(obj.stream)
		return
	}
	select {
	case p.objects <- obj:
		return
	default:
		obj.stream.Close()
		p.MarkClosed(obj.stream)
	}
}

func (p *SmuxPool) GetStat() *SmuxPoolStat {
	stat := &SmuxPoolStat{
		Addr:                    p.target,
		InflightStreams:         0,
		InflightStreamsReported: 0,
		TotalSessions:           0,
		StreamsPerSession:       make(map[string]int, p.cfg.ConnsPerAddr),
	}
	p.sessionsLock.RLock()
	stat.TotalSessions = len(p.sessions)
	stat.InflightStreamsReported = p.inflightStreamNum()
	for _, sess := range p.sessions {
		streams := sess.NumStreams()
		stat.InflightStreams += streams
		stat.StreamsPerSession[sess.LocalAddr().String()] += streams
	}
	p.sessionsLock.RUnlock()
	return stat
}
