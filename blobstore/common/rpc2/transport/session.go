package transport

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultAcceptBacklog = 1024
	openCloseTimeout     = 30 * time.Second // stream open/close timeout
)

// define frame class
type CLASSID uint8

const (
	CLSCTRL CLASSID = iota
	CLSDATA
)

var (
	ErrInvalidProtocol = errors.New("transport: invalid protocol")
	ErrConsumed        = errors.New("transport: peer consumed more than sent")
	ErrGoAway          = errors.New("transport: stream id overflows")
	ErrTimeout         = errors.New("transport: timeout")
	ErrWouldBlock      = errors.New("transport: operation would block on IO")
	ErrAllocOversize   = errors.New("transport: allocator was oversize")
	ErrFrameOversize   = errors.New("transport: frame was oversize")
	ErrFrameOdd        = errors.New("transport: frame was odd")
)

type writeRequest struct {
	frame    *FrameWrite
	deadline time.Time
	result   chan writeResult
}

type writeResult struct {
	n   int
	err error
}

type writeDealine struct {
	time time.Time
	wait <-chan time.Time
}

// Session defines a multiplexed connection for streams
type Session struct {
	conn Conn

	config           *Config
	nextStreamID     uint32 // next stream identifier
	nextStreamIDLock sync.Mutex

	bucket       int32         // token bucket
	bucketNotify chan struct{} // used for waiting for tokens

	streams    map[uint32]*Stream // all streams in this session
	streamLock sync.RWMutex       // locks streams

	die     chan struct{} // flag session has died
	dieOnce sync.Once

	// socket error handling
	socketReadError      atomic.Value
	socketWriteError     atomic.Value
	chSocketReadError    chan struct{}
	chSocketWriteError   chan struct{}
	socketReadErrorOnce  sync.Once
	socketWriteErrorOnce sync.Once

	chAccepts chan *Stream

	dataReady int32 // flag data has arrived
	pingpong  chan struct{}

	goAway int32 // flag id exhausted

	deadline atomic.Value

	ctrl         chan writeRequest // a ctrl frame for writing
	writes       chan writeRequest
	resultChPool sync.Pool
}

func newSession(config *Config, conn Conn, client bool) *Session {
	s := new(Session)
	s.die = make(chan struct{})
	s.conn = conn
	s.config = config
	s.streams = make(map[uint32]*Stream)
	s.chAccepts = make(chan *Stream, defaultAcceptBacklog)
	s.bucket = int32(config.MaxReceiveBuffer)
	s.bucketNotify = make(chan struct{}, 1)
	s.pingpong = make(chan struct{}, 1)
	s.ctrl = make(chan writeRequest, 1)
	s.writes = make(chan writeRequest)
	s.resultChPool = sync.Pool{New: func() interface{} {
		return make(chan writeResult, 1)
	}}
	s.chSocketReadError = make(chan struct{})
	s.chSocketWriteError = make(chan struct{})

	if client {
		s.nextStreamID = 1
	} else {
		s.nextStreamID = 0
	}

	go s.recvLoop()
	go s.sendLoop()
	if !config.KeepAliveDisabled {
		go s.keepalive(client)
	}
	return s
}

// OpenStream is used to create a new stream
func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, io.ErrClosedPipe
	}

	// generate stream id
	s.nextStreamIDLock.Lock()
	if s.goAway > 0 {
		s.nextStreamIDLock.Unlock()
		return nil, ErrGoAway
	}

	s.nextStreamID += 2
	sid := s.nextStreamID
	if sid == sid%2 { // stream-id overflows
		s.goAway = 1
		s.nextStreamIDLock.Unlock()
		return nil, ErrGoAway
	}
	s.nextStreamIDLock.Unlock()

	stream := newStream(sid, s.config.MaxFrameSize, s)

	frame, err := s.newFrameWrite(cmdSYN, sid, 0)
	if err != nil {
		return nil, err
	}
	if _, err := s.writeFrame(frame); err != nil {
		return nil, err
	}

	select {
	case <-s.chSocketReadError:
		return nil, s.socketReadError.Load().(error)
	case <-s.chSocketWriteError:
		return nil, s.socketWriteError.Load().(error)
	case <-s.die:
		return nil, io.ErrClosedPipe
	default:
		s.streamLock.Lock()
		s.streams[sid] = stream
		s.streamLock.Unlock()
		return stream, nil
	}
}

// AcceptStream is used to block until the next available stream
// is ready to be accepted.
func (s *Session) AcceptStream() (*Stream, error) {
	var deadline <-chan time.Time
	if d, ok := s.deadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case stream := <-s.chAccepts:
		return stream, nil
	case <-deadline:
		return nil, ErrTimeout
	case <-s.chSocketReadError:
		return nil, s.socketReadError.Load().(error)
	case <-s.die:
		return nil, io.ErrClosedPipe
	}
}

// Close is used to close the session and all streams.
func (s *Session) Close() error {
	var once bool
	s.dieOnce.Do(func() {
		close(s.die)
		once = true
	})

	if once {
		s.streamLock.Lock()
		for k := range s.streams {
			s.streams[k].sessionClose()
		}
		s.streamLock.Unlock()
		return s.conn.Close()
	} else {
		return io.ErrClosedPipe
	}
}

// CloseChan can be used by someone who wants to be notified immediately when this
// session is closed
func (s *Session) CloseChan() <-chan struct{} {
	return s.die
}

// notifyBucket notifies recvLoop that bucket is available
func (s *Session) notifyBucket() {
	select {
	case s.bucketNotify <- struct{}{}:
	default:
	}
}

func (s *Session) notifyReadError(err error) {
	s.socketReadErrorOnce.Do(func() {
		s.socketReadError.Store(err)
		close(s.chSocketReadError)
	})
}

func (s *Session) notifyWriteError(err error) {
	s.socketWriteErrorOnce.Do(func() {
		s.socketWriteError.Store(err)
		close(s.chSocketWriteError)
	})
}

// IsClosed does a safe check to see if we have shutdown
func (s *Session) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// NumStreams returns the number of currently open streams
func (s *Session) NumStreams() int {
	if s.IsClosed() {
		return 0
	}
	s.streamLock.RLock()
	defer s.streamLock.RUnlock()
	return len(s.streams)
}

// SetDeadline sets a deadline used by Accept* calls.
// A zero time value disables the deadline.
func (s *Session) SetDeadline(t time.Time) error {
	s.deadline.Store(t)
	return nil
}

// LocalAddr satisfies net.Conn interface
func (s *Session) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// RemoteAddr satisfies net.Conn interface
func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// notify the session that a stream has closed
func (s *Session) streamClosed(sid uint32) {
	s.streamLock.Lock()
	if n := s.streams[sid].recycleTokens(); n > 0 { // return remaining tokens to the bucket
		if atomic.AddInt32(&s.bucket, int32(n)) > 0 {
			s.notifyBucket()
		}
	}
	delete(s.streams, sid)
	s.streamLock.Unlock()
}

// returnTokens is called by stream to return token after read
func (s *Session) returnTokens(n int) {
	if atomic.AddInt32(&s.bucket, int32(n)) > 0 {
		s.notifyBucket()
	}
}

// recvLoop keeps on reading from underlying connection if tokens are available
func (s *Session) recvLoop() {
	var (
		err error
		hdr rawHeader
		upd updHeader

		pshLen    int
		pshBuffer AssignedBuffer
		pshFrame  *FrameRead
	)

	for {
		for atomic.LoadInt32(&s.bucket) <= 0 && !s.IsClosed() {
			select {
			case <-s.bucketNotify:
			case <-s.die:
				return
			}
		}

		// read header first
		if _, err = io.ReadFull(s.conn, hdr[:]); err == nil {
			atomic.StoreInt32(&s.dataReady, 1)
			if hdr.Version() != byte(s.config.Version) {
				s.notifyReadError(ErrInvalidProtocol)
				return
			}
			sid := hdr.StreamID()
			switch hdr.Cmd() {
			case cmdPIN:
				s.pong()
			case cmdPON:
				select {
				case s.pingpong <- struct{}{}:
				default:
				}
			case cmdSYN:
				s.streamLock.Lock()
				if _, ok := s.streams[sid]; !ok {
					stream := newStream(sid, s.config.MaxFrameSize, s)
					s.streams[sid] = stream
					select {
					case s.chAccepts <- stream:
					case <-s.die:
					}
				}
				s.streamLock.Unlock()
			case cmdFIN:
				s.streamLock.RLock()
				if stream, ok := s.streams[sid]; ok {
					stream.fin()
					stream.notifyReadEvent()
				}
				s.streamLock.RUnlock()
			case cmdPSH:
				if pshLen = int(hdr.Length()); pshLen > 0 {
					pshBuffer, err = s.conn.ReadBuffer(pshLen)
					if err != nil {
						s.notifyReadError(err)
						return
					}
					pshFrame = s.newFrameRead(pshBuffer)

					s.streamLock.RLock()
					if stream, ok := s.streams[sid]; ok {
						stream.pushFrameRead(pshFrame)
						atomic.AddInt32(&s.bucket, -int32(pshLen))
						stream.notifyReadEvent()
					} else {
						pshFrame.Close()
					}
					s.streamLock.RUnlock()
				}
			case cmdUPD:
				if _, err = io.ReadFull(s.conn, upd[:]); err == nil {
					s.streamLock.Lock()
					if stream, ok := s.streams[sid]; ok {
						stream.update(upd.Consumed(), upd.Window())
					}
					s.streamLock.Unlock()
				} else {
					s.notifyReadError(err)
					return
				}
			default:
				s.notifyReadError(ErrInvalidProtocol)
				return
			}
		} else {
			s.notifyReadError(err)
			return
		}
	}
}

func (s *Session) ping(ticker *time.Ticker) {
	deadline := writeDealine{
		time: time.Now().Add(s.config.KeepAliveInterval),
		wait: ticker.C,
	}
	frame, err := s.newFrameWrite(cmdPIN, 0, 0)
	if err == nil {
		s.writeFrameInternal(frame, deadline, CLSCTRL)
		frame.Close()
	}
}

func (s *Session) pong() {
	frame, err := s.newFrameWrite(cmdPON, 0, 0)
	if err == nil {
		s.writeFrame(frame)
	}
}

func (s *Session) keepalive(client bool) {
	tickerTimeout := time.NewTicker(s.config.KeepAliveTimeout)
	defer tickerTimeout.Stop()
	if !client { // server side
		for {
			select {
			case <-tickerTimeout.C:
				if !atomic.CompareAndSwapInt32(&s.dataReady, 1, 0) {
					// recvLoop may block while bucket is 0, in this case,
					// session should not be closed.
					if atomic.LoadInt32(&s.bucket) > 0 {
						s.Close()
						return
					}
				}
			case <-s.die:
				return
			}
		}
	}

	tickerPing := time.NewTicker(s.config.KeepAliveInterval)
	defer tickerPing.Stop()
	alive := false
	for {
		select {
		case <-tickerPing.C:
			s.ping(tickerPing)
			s.notifyBucket() // force a signal to the recvLoop
		case <-tickerTimeout.C:
			if !atomic.CompareAndSwapInt32(&s.dataReady, 1, 0) {
				// recvLoop may block while bucket is 0, in this case,
				// session should not be closed.
				if atomic.LoadInt32(&s.bucket) > 0 && !alive {
					s.Close()
					return
				}
			}
			alive = false
		case <-s.pingpong:
			alive = true
		case <-s.die:
			return
		}
	}
}

func (s *Session) sendLoop() {
	writev, isWritev := s.conn.(WriteBuffers)

	var idx, n, nn int
	var err error
	var request writeRequest

	const maxLen = 32
	maxSize := s.config.MaxReceiveBuffer / 2
	var deadline time.Time
	requests := make([]writeRequest, 0, maxLen)
	buffers := make([]AssignedBuffer, 0, maxLen)
	size := 0
	defer func() {
		for idx = range requests {
			requests[idx].frame.unlock()
		}
		requests = requests[:0]
		buffers = buffers[:0]
	}()

	for {
		select {
		case request = <-s.ctrl:
		case <-s.die:
			return
		default:
			select {
			case <-s.die:
				return
			case request = <-s.ctrl:
			case request = <-s.writes:
			}
		}
		if !request.frame.tryLock() { // closed
			continue
		}
		request.frame.writeHeader()

		if !isWritev {
			s.conn.SetWriteDeadline(request.deadline)
			n, err = s.conn.WriteBuffer(request.frame.ab)
			if n -= headerSize; n < 0 {
				n = 0
			}
			request.frame.unlock()
			request.result <- writeResult{n: n, err: err}

			// store conn error
			if err != nil {
				s.notifyWriteError(err)
				return
			}
			continue
		}

		requests = requests[:0]
		buffers = buffers[:0]
		size = 0
		deadline = time.Time{}

	LoopMore: // try to wait more frame
		for {
			requests = append(requests, request)
			buffers = append(buffers, request.frame.ab)
			size += request.frame.off
			deadline = longestTime(deadline, request.deadline)
			if len(requests) >= maxLen || size >= maxSize {
				break
			}

			select {
			case request = <-s.writes:
			default:
				break LoopMore
			}

			if !request.frame.tryLock() {
				break
			}
			request.frame.writeHeader()
		}

		s.conn.SetWriteDeadline(deadline)
		nn, err = writev.WriteBuffers(buffers)
		if err == nil && nn != size {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.notifyWriteError(err)
			return
		}

		for idx = range requests {
			request = requests[idx]
			n = request.frame.Len()
			request.frame.unlock()
			request.result <- writeResult{n: n}
		}
	}
}

// writeFrame writes the frame to the underlying connection
// and returns the number of bytes written if successful
func (s *Session) writeFrame(f *FrameWrite) (n int, err error) {
	timer := time.NewTimer(openCloseTimeout)
	defer timer.Stop()
	defer f.Close()
	deadline := writeDealine{
		time: time.Now().Add(openCloseTimeout),
		wait: timer.C,
	}
	return s.writeFrameInternal(f, deadline, CLSCTRL)
}

// internal writeFrame version to support deadline used in keepalive
func (s *Session) writeFrameInternal(f *FrameWrite, deadline writeDealine, class CLASSID) (int, error) {
	req := writeRequest{
		frame:    f,
		deadline: deadline.time,
		result:   s.resultChPool.Get().(chan writeResult),
	}
	writeCh := s.writes
	if class == CLSCTRL {
		writeCh = s.ctrl
	}

	ctx := f.Context()
	select {
	case <-deadline.wait:
		return 0, ErrTimeout
	default:
		select {
		case writeCh <- req:
		case <-s.die:
			return 0, io.ErrClosedPipe
		case <-s.chSocketWriteError:
			return 0, s.socketWriteError.Load().(error)
		case <-deadline.wait:
			return 0, ErrTimeout
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	select {
	case result := <-req.result:
		s.resultChPool.Put(req.result)
		return result.n, result.err
	case <-s.die:
		return 0, io.ErrClosedPipe
	case <-s.chSocketWriteError:
		return 0, s.socketWriteError.Load().(error)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (s *Session) newFrameWrite(cmd byte, sid uint32, size int) (*FrameWrite, error) {
	buffer, err := s.conn.Allocator().Alloc(size + headerSize)
	if err != nil {
		return nil, err
	}
	buffer.Written(headerSize)
	return &FrameWrite{
		ver: byte(s.config.Version),
		cmd: cmd,
		sid: sid,

		ab:   buffer,
		off:  headerSize,
		data: buffer.Bytes()[:],
	}, nil
}

func (s *Session) newFrameRead(buffer AssignedBuffer) *FrameRead {
	return &FrameRead{
		ab:   buffer,
		data: buffer.Bytes()[:buffer.Len()],
	}
}

func longestTime(t time.Time, other time.Time) time.Time {
	if t.IsZero() || other.After(t) {
		return other
	}
	return t
}
