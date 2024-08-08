package transport

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Stream implements net.Conn
type Stream struct {
	id   uint32
	sess *Session

	frames    *ringFrame
	frameSize int

	// notify a read event
	chReadEvent chan struct{}

	// flag the stream has closed
	die     chan struct{}
	dieOnce sync.Once

	// FIN command
	chFinEvent   chan struct{}
	finEventOnce sync.Once

	// deadlines
	readDeadline  atomic.Value
	writeDeadline atomic.Value

	// per stream sliding window control
	numRead    uint32 // number of consumed bytes
	numWritten uint32 // count num of bytes written
	incr       uint32 // counting for sending

	// UPD command
	peerConsumed uint32        // num of bytes the peer has consumed
	peerWindow   uint32        // peer window, initialized to 256KB, updated by peer
	chUpdate     chan struct{} // notify of remote data consuming and window update
}

// newStream initiates a Stream struct
func newStream(id uint32, frameSize int, sess *Session) *Stream {
	s := new(Stream)
	s.id = id
	s.frames = newRingFrame()
	s.chReadEvent = make(chan struct{}, 1)
	s.chUpdate = make(chan struct{}, 1)
	s.frameSize = frameSize
	s.sess = sess
	s.die = make(chan struct{})
	s.chFinEvent = make(chan struct{})
	s.peerWindow = initialPeerWindow // set to initial window size
	return s
}

// ID returns the unique stream ID.
func (s *Stream) ID() uint32 {
	return s.id
}

// MaxPayloadSize returns max payload size of frame
func (s *Stream) MaxPayloadSize() int {
	return s.frameSize - headerSize
}

// AllocFrame returns a writable frame
func (s *Stream) AllocFrame(size int) (*FrameWrite, error) {
	if size > s.MaxPayloadSize() || size <= 0 {
		return nil, ErrFrameOversize
	}
	return s.sess.newFrameWrite(cmdPSH, s.id, size)
}

// SizedReader the size must be some full frames,
// should close it whatever happens.
func (s *Stream) SizedReader(size int) *SizedReader {
	return s.NewSizedReader(size, nil)
}

// ReadFrame returns frame data, closed by caller
func (s *Stream) ReadFrame() (*FrameRead, error) {
	for {
		f, err := s.tryReadFrame()
		if err == ErrWouldBlock {
			if ew := s.waitRead(); ew != nil {
				return nil, ew
			}
		} else {
			return f, err
		}
	}
}

// tryReadFrame is the nonblocking version of Read
func (s *Stream) tryReadFrame() (*FrameRead, error) {
	if s.sess.config.Version == 2 {
		return s.tryReadFramev2()
	}

	if f := s.frames.Dequeue(); f != nil {
		s.sess.returnTokens(f.Len())
		return f, nil
	}

	select {
	case <-s.die:
		return nil, io.EOF
	default:
		return nil, ErrWouldBlock
	}
}

func (s *Stream) tryReadFramev2() (*FrameRead, error) {
	var notifyConsumed uint32
	f := s.frames.Dequeue()
	if f == nil {
		select {
		case <-s.die:
			return nil, io.EOF
		default:
			return nil, ErrWouldBlock
		}
	}

	// in an ideal environment:
	// if more than half of buffer has consumed, send read ack to peer
	// based on round-trip time of ACK, continous flowing data
	// won't slow down because of waiting for ACK, as long as the
	// consumer keeps on reading data
	// s.numRead == n also notify window at the first read
	n := f.Len()
	s.numRead += uint32(n)
	s.incr += uint32(n)
	if s.incr >= uint32(s.sess.config.MaxStreamBuffer/2) || s.numRead == uint32(n) {
		notifyConsumed = s.numRead
		s.incr = 0
	}

	s.sess.returnTokens(n)
	if notifyConsumed > 0 {
		if err := s.sendWindowUpdate(notifyConsumed); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func (s *Stream) sendWindowUpdate(consumed uint32) error {
	var timer *time.Timer
	var deadline writeDealine
	if d, ok := s.readDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer = time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline.time = d
		deadline.wait = timer.C
	}

	var hdr updHeader
	frame, err := s.sess.newFrameWrite(cmdUPD, s.id, len(hdr))
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(hdr[:], consumed)
	binary.LittleEndian.PutUint32(hdr[4:], uint32(s.sess.config.MaxStreamBuffer))
	frame.Write(hdr[:])
	_, err = s.sess.writeFrameInternal(frame, deadline, CLSDATA)
	return err
}

func (s *Stream) waitRead() error {
	var timer *time.Timer
	var deadline <-chan time.Time
	if d, ok := s.readDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer = time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case <-s.chReadEvent:
		return nil
	case <-s.chFinEvent:
		// BUG(xtaci): Fix for https://github.com/xtaci/smux/issues/82
		if s.frames.HasData() {
			return nil
		}
		return io.EOF
	case <-s.sess.chSocketReadError:
		return s.sess.socketReadError.Load().(error)
	case <-deadline:
		return ErrTimeout
	case <-s.die:
		return io.ErrClosedPipe
	}
}

func (s *Stream) SizedWrite(r io.Reader, size int) (n int, err error) {
	maxPayloadSize := s.MaxPayloadSize()
	var nn int
	var fw *FrameWrite
	for size > 0 {
		alloc := size
		if alloc > maxPayloadSize {
			alloc = maxPayloadSize
		}

		fw, err = s.AllocFrame(alloc)
		if err != nil {
			return
		}
		_, err = fw.ReadFrom(r)
		if err != nil {
			fw.Close()
			return
		}

		nn, err = s.WriteFrame(fw)
		if err != nil {
			fw.Close()
			return
		}
		fw.Close()

		n += nn
		size -= nn
	}
	return
}

// WriteFrame close frame by caller if has error.
func (s *Stream) WriteFrame(frame *FrameWrite) (n int, err error) {
	// check empty input
	if frame.Len() == 0 {
		return 0, nil
	}
	// check if stream has closed
	select {
	case <-s.die:
		return 0, io.ErrClosedPipe
	default:
	}

	var deadline writeDealine
	if d, ok := s.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline.time = d
		deadline.wait = timer.C
	}

	if s.sess.config.Version == 2 {
		return s.writeFrameV2(frame, deadline)
	}

	sent, err := s.sess.writeFrameInternal(frame, deadline, CLSDATA)
	s.numWritten += uint32(sent)
	return sent, err
}

func (s *Stream) writeFrameV2(frame *FrameWrite, deadline writeDealine) (n int, err error) {
	for {
		// per stream sliding window control
		// [.... [consumed... numWritten] ... win... ]
		// [.... [consumed...................+rmtwnd]]
		//
		// note:
		// even if uint32 overflow, this math still works:
		// eg1: uint32(0) - uint32(math.MaxUint32) = 1
		// eg2: int32(uint32(0) - uint32(1)) = -1
		// security check for misbehavior
		inflight := int32(s.numWritten - atomic.LoadUint32(&s.peerConsumed))
		if inflight < 0 {
			return 0, ErrConsumed
		}

		win := int32(atomic.LoadUint32(&s.peerWindow)) - inflight
		if win >= int32(frame.Len()) || s.numWritten == 0 {
			sent, err := s.sess.writeFrameInternal(frame, deadline, CLSDATA)
			s.numWritten += uint32(sent)
			return sent, err
		}

		// wait until stream closes, window changes or deadline reached
		// this blocking behavior will inform upper layer to do flow control
		select {
		case <-s.chFinEvent: // if fin arrived, future window update is impossible
			return 0, io.EOF
		case <-s.die:
			return 0, io.ErrClosedPipe
		case <-deadline.wait:
			return 0, ErrTimeout
		case <-s.sess.chSocketWriteError:
			return 0, s.sess.socketWriteError.Load().(error)
		case <-s.chUpdate:
			continue
		}
	}
}

// Close implements net.Conn
func (s *Stream) Close() error {
	var once bool
	var err error
	s.dieOnce.Do(func() {
		close(s.die)
		once = true
	})

	var frame *FrameWrite
	if once {
		s.sess.streamClosed(s.id)

		frame, err = s.sess.newFrameWrite(cmdFIN, s.id, 0)
		if err != nil {
			return err
		}
		_, err = s.sess.writeFrame(frame)
		return err
	} else {
		return io.ErrClosedPipe
	}
}

// CloseChan returns a readonly chan which can be readable
// when the stream is to be closed.
func (s *Stream) CloseChan() <-chan struct{} {
	return s.die
}

// IsClosed does a safe check to see if we have shutdown.
func (s *Stream) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// SetReadDeadline sets the read deadline as defined by
// net.Conn.SetReadDeadline.
// A zero time value disables the deadline.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	s.notifyReadEvent()
	return nil
}

// SetWriteDeadline sets the write deadline as defined by
// net.Conn.SetWriteDeadline.
// A zero time value disables the deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	return nil
}

// SetDeadline sets both read and write deadlines as defined by
// net.Conn.SetDeadline.
// A zero time value disables the deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// session closes
func (s *Stream) sessionClose() { s.dieOnce.Do(func() { close(s.die) }) }

// LocalAddr satisfies net.Conn interface
func (s *Stream) LocalAddr() net.Addr {
	return s.sess.LocalAddr()
}

// RemoteAddr satisfies net.Conn interface
func (s *Stream) RemoteAddr() net.Addr {
	return s.sess.RemoteAddr()
}

func (s *Stream) pushFrameRead(f *FrameRead) {
	s.frames.Enqueue(f)
}

// recycleTokens transform remaining bytes to tokens(will truncate buffer)
func (s *Stream) recycleTokens() (n int) {
	return s.frames.Recycle()
}

// notify read event
func (s *Stream) notifyReadEvent() {
	select {
	case s.chReadEvent <- struct{}{}:
	default:
	}
}

// update command
func (s *Stream) update(consumed uint32, window uint32) {
	atomic.StoreUint32(&s.peerConsumed, consumed)
	atomic.StoreUint32(&s.peerWindow, window)
	select {
	case s.chUpdate <- struct{}{}:
	default:
	}
}

// mark this stream has been closed in protocol
func (s *Stream) fin() {
	s.finEventOnce.Do(func() {
		close(s.chFinEvent)
	})
}

type SizedReader struct {
	n int
	s *Stream
	f *FrameRead

	finished bool

	once sync.Once
	err  error
}

func (s *Stream) NewSizedReader(size int, f *FrameRead) *SizedReader {
	return &SizedReader{n: size, s: s, f: f}
}

func (r *SizedReader) tryNextFrame() error {
	if r.err != nil {
		return r.err
	}
	if r.n <= 0 {
		r.finished = true
		r.err = io.EOF
		if r.f != nil && r.f.Len() == 0 {
			r.f.Close()
		}
		return r.err
	}

	for r.f == nil || r.f.Len() == 0 {
		// close last frame
		if r.f != nil && r.f.Len() == 0 {
			r.f.Close()
		}
		r.f, r.err = r.s.ReadFrame()
		if r.err != nil {
			return r.err
		}
		if r.n-r.f.Len() < 0 {
			r.err = ErrFrameOdd
			return r.err
		}
	}
	if r.n == r.f.Len() { // the final frame
		r.finished = true
	}
	return nil
}

func (r *SizedReader) Read(p []byte) (int, error) {
	if err := r.tryNextFrame(); err != nil {
		return 0, err
	}
	n, err := r.f.Read(p)
	r.n -= n
	r.err = err
	return n, err
}

func (r *SizedReader) WriteTo(w io.Writer) (int64, error) {
	var nn int64
	for {
		if err := r.tryNextFrame(); err != nil {
			return nn, err
		}
		n, err := r.f.WriteTo(w)
		r.n -= int(n)
		r.err = err
		nn += n
		if r.n == 0 { // return nil if read full
			return nn, nil
		}
		if err != nil {
			return nn, err
		}
	}
}

func (r *SizedReader) Close() (err error) {
	r.tryNextFrame()
	r.once.Do(func() {
		if r.err == nil {
			r.err = io.EOF
		}
		if r.f != nil {
			r.f.Close()
		}
		r.s = nil
		r.f = nil
	})
	return
}

// Finished has no frame in stream.
func (r *SizedReader) Finished() bool {
	return r.finished
}
