// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ClientType is a type alias to represent the type of this region client
type ClientType string

type canDeserializeCellBlocks interface {
	// DeserializeCellBlocks populates passed protobuf message with results
	// deserialized from the reader and returns number of bytes read or error.
	DeserializeCellBlocks(proto.Message, []byte) (uint32, error)
}

type canSerializeCellBlocks interface {
	// SerializeCellBlocks serializes RPC into protobuf for metadata
	// as well as cellblocks for payload.
	SerializeCellBlocks() (proto.Message, [][]byte, uint32)
	// CellBlocksEnabled returns true if cellblocks are enabled for this RPC
	CellBlocksEnabled() bool
}

var (
	// ErrMissingCallID is used when HBase sends us a response message for a
	// request that we didn't send
	ErrMissingCallID = ServerError{errors.New("got a response with a nonsensical call ID")}

	// ErrClientClosed is returned to rpcs when Close() is called or when client
	// died because of failed send or receive
	ErrClientClosed = ServerError{errors.New("client is closed")}

	// If a Java exception listed here is returned by HBase, the client should
	// reestablish region and attempt to resend the RPC message, potentially via
	// a different region client.
	// The value of exception should be contained in the stack trace.
	javaRegionExceptions = map[string]string{
		"org.apache.hadoop.hbase.NotServingRegionException":       "",
		"org.apache.hadoop.hbase.exceptions.RegionMovedException": "",
		"java.io.IOException": "Cannot append; log is closed",
	}

	// If a Java exception listed here is returned by HBase, the client should
	// backoff and resend the RPC message to the same region and region server
	// The value of exception should be contained in the stack trace.
	javaRetryableExceptions = map[string]string{
		"org.apache.hadoop.hbase.CallQueueTooBigException":          "",
		"org.apache.hadoop.hbase.exceptions.RegionOpeningException": "",
		"org.apache.hadoop.hbase.ipc.ServerNotRunningYetException":  "",
		"org.apache.hadoop.hbase.quotas.RpcThrottlingException":     "",
		"org.apache.hadoop.hbase.RetryImmediatelyException":         "",
		"org.apache.hadoop.hbase.RegionTooBusyException":            "",
	}

	// javaServerExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the RegionClient will be closed and a new
	// one should be established.
	// The value of exception should be contained in the stack trace.
	javaServerExceptions = map[string]string{
		"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException": "",
		"org.apache.hadoop.hbase.regionserver.RegionServerStoppedException": "",
	}
)

const (
	//DefaultLookupTimeout is the default region lookup timeout
	DefaultLookupTimeout = 30 * time.Second
	//DefaultReadTimeout is the default region read timeout
	DefaultReadTimeout = 30 * time.Second
	// RegionClient is a ClientType that means this will be a normal client
	RegionClient = ClientType("ClientService")

	// MasterClient is a ClientType that means this client will talk to the
	// master server
	MasterClient = ClientType("MasterService")
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		var b []byte
		return b
	},
}

func newBuffer(size int) []byte {
	b := bufferPool.Get().([]byte)
	if cap(b) < size {
		doublecap := 2 * cap(b)
		if doublecap > size {
			return make([]byte, size, doublecap)
		}
		return make([]byte, size)
	}
	return b[:size]
}

func freeBuffer(b []byte) {
	bufferPool.Put(b[:0]) // nolint:staticcheck
}

// ServerError is an error that this region.Client can't recover from.
// The connection to the RegionServer has to be closed and all queued and
// outstanding RPCs will be failed / retried.
type ServerError struct {
	error
}

func formatErr(e interface{}, err error) string {
	if err == nil {
		return fmt.Sprintf("%T", e)
	}
	return fmt.Sprintf("%T: %s", e, err.Error())
}

func (e ServerError) Error() string {
	return formatErr(e, e.error)
}

// RetryableError is an error that indicates the RPC should be retried after backoff
// because the error is transient (e.g. a region being momentarily unavailable).
type RetryableError struct {
	error
}

func (e RetryableError) Error() string {
	return formatErr(e, e.error)
}

// NotServingRegionError is an error that indicates the client should
// reestablish the region and retry the RPC potentially via a different client
type NotServingRegionError struct {
	error
}

func (e NotServingRegionError) Error() string {
	return formatErr(e, e.error)
}

// client manages a connection to a RegionServer.
type client struct {
	conn net.Conn

	// Address of the RegionServer.
	addr  string
	ctype ClientType

	// dialOnce used for concurrent calls to Dial
	dialOnce sync.Once
	// failOnce used for concurrent calls to fail
	failOnce sync.Once

	rpcs chan hrpc.Call
	done chan struct{}

	// sent contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sentM sync.Mutex // protects sent
	sent  map[uint32]hrpc.Call

	// inFlight is number of rpcs sent to regionserver awaiting response
	inFlightM sync.Mutex // protects inFlight and SetReadDeadline
	inFlight  uint32

	id uint32

	rpcQueueSize  int
	flushInterval time.Duration
	effectiveUser string

	// readTimeout is the maximum amount of time to wait for regionserver reply
	readTimeout time.Duration

	// compressor for cellblocks. if nil, then no compression
	compressor *compressor
}

// QueueRPC will add an rpc call to the queue for processing by the writer goroutine
func (c *client) QueueRPC(rpc hrpc.Call) {
	if b, ok := rpc.(hrpc.Batchable); ok && c.rpcQueueSize > 1 && !b.SkipBatch() {
		// queue up the rpc
		select {
		case <-rpc.Context().Done():
			// rpc timed out before being processed
		case <-c.done:
			returnResult(rpc, nil, ErrClientClosed)
		case c.rpcs <- rpc:
		}
	} else {
		if err := c.trySend(rpc); err != nil {
			returnResult(rpc, nil, err)
		}
	}
}

// Close asks this region.Client to close its connection to the RegionServer.
// All queued and outstanding RPCs, if any, will be failed as if a connection
// error had happened.
func (c *client) Close() {
	c.fail(ErrClientClosed)
}

// Addr returns address of the region server the client is connected to
func (c *client) Addr() string {
	return c.addr
}

// String returns a string represintation of the current region client
func (c *client) String() string {
	return fmt.Sprintf("RegionClient{Addr: %s}", c.addr)
}

func (c *client) inFlightUp() error {
	c.inFlightM.Lock()
	c.inFlight++
	// we expect that at least the last request can be completed within readTimeout
	if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		c.inFlightM.Unlock()
		return err
	}
	c.inFlightM.Unlock()
	return nil
}

func (c *client) inFlightDown() error {
	c.inFlightM.Lock()
	c.inFlight--
	// reset read timeout if we are not waiting for any responses
	// in order to prevent from closing this client if there are no request
	if c.inFlight == 0 {
		if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
			c.inFlightM.Unlock()
			return err
		}
	}
	c.inFlightM.Unlock()
	return nil
}

func (c *client) fail(err error) {
	c.failOnce.Do(func() {
		if err != ErrClientClosed {
			log.WithFields(log.Fields{
				"client": c,
				"err":    err,
			}).Error("error occured, closing region client")
		}

		// we don't close c.rpcs channel to make it block in select of QueueRPC
		// and avoid dealing with synchronization of closing it while someone
		// might be sending to it. Go's GC will take care of it.

		// tell goroutines to stop
		close(c.done)

		// close connection to the regionserver
		// to let it know that we can't receive anymore
		// and fail all the rpcs being sent
		if c.conn != nil {
			c.conn.Close()
		}

		c.failSentRPCs()
	})
}

func (c *client) failSentRPCs() {
	// channel is closed, clean up awaiting rpcs
	c.sentM.Lock()
	sent := c.sent
	c.sent = make(map[uint32]hrpc.Call)
	c.sentM.Unlock()

	log.WithFields(log.Fields{
		"client": c,
		"count":  len(sent),
	}).Debug("failing awaiting RPCs")

	// send error to awaiting rpcs
	for _, rpc := range sent {
		returnResult(rpc, nil, ErrClientClosed)
	}
}

func (c *client) registerRPC(rpc hrpc.Call) uint32 {
	currID := atomic.AddUint32(&c.id, 1)
	c.sentM.Lock()
	c.sent[currID] = rpc
	c.sentM.Unlock()
	return currID
}

func (c *client) unregisterRPC(id uint32) hrpc.Call {
	c.sentM.Lock()
	rpc := c.sent[id]
	delete(c.sent, id)
	c.sentM.Unlock()
	return rpc
}

func (c *client) processRPCs() {
	// TODO: flush when the size is too large
	// TODO: if multi has only one call, send that call instead
	m := newMulti(c.rpcQueueSize)
	defer func() {
		m.returnResults(nil, ErrClientClosed)
	}()

	flush := func() {
		if log.GetLevel() == log.DebugLevel {
			log.WithFields(log.Fields{
				"len":  m.len(),
				"addr": c.Addr(),
			}).Debug("flushing MultiRequest")
		}
		if err := c.trySend(m); err != nil {
			m.returnResults(nil, err)
		}
		m = newMulti(c.rpcQueueSize)
	}

	for {
		// first loop is to accomodate request heavy workload
		// it will batch as long as conccurent writers are sending
		// new rpcs or until multi is filled up
		for {
			select {
			case <-c.done:
				return
			case rpc := <-c.rpcs:
				// have things queued up, batch them
				if !m.add(rpc) {
					// can still put more rpcs into batch
					continue
				}
			default:
				// no more rpcs queued up
			}
			break
		}

		if l := m.len(); l == 0 {
			// wait for the next batch
			select {
			case <-c.done:
				return
			case rpc := <-c.rpcs:
				m.add(rpc)
			}
			continue
		} else if l == c.rpcQueueSize || c.flushInterval == 0 {
			// batch is full, flush
			flush()
			continue
		}

		// second loop is to accomodate less frequent callers
		// that would like to maximize their batches at the expense
		// of waiting for flushInteval
		timer := time.NewTimer(c.flushInterval)
		for {
			select {
			case <-c.done:
				return
			case <-timer.C:
				// time to flush
			case rpc := <-c.rpcs:
				if !m.add(rpc) {
					// can still put more rpcs into batch
					continue
				}
				// batch is full
				if !timer.Stop() {
					<-timer.C
				}
			}
			break
		}
		flush()
	}
}

func returnResult(c hrpc.Call, msg proto.Message, err error) {
	if m, ok := c.(*multi); ok {
		m.returnResults(msg, err)
	} else {
		c.ResultChan() <- hrpc.RPCResult{Msg: msg, Error: err}
	}
}

func (c *client) trySend(rpc hrpc.Call) error {
	select {
	case <-c.done:
		// An unrecoverable error has occured,
		// region client has been stopped,
		// don't send rpcs
		return ErrClientClosed
	case <-rpc.Context().Done():
		// If the deadline has been exceeded, don't bother sending the
		// request. The function that placed the RPC in our queue should
		// stop waiting for a result and return an error.
		return nil
	default:
		if id, err := c.send(rpc); err != nil {
			if _, ok := err.(ServerError); ok {
				c.fail(err)
			}
			if r := c.unregisterRPC(id); r != nil {
				// we are the ones to unregister the rpc,
				// return err to notify client of it
				return err
			}
		}
		return nil
	}
}

func (c *client) receiveRPCs() {
	for {
		select {
		case <-c.done:
			return
		default:
			if err := c.receive(); err != nil {
				if _, ok := err.(ServerError); ok {
					// fail the client and let the callers establish a new one
					c.fail(err)
					return
				}
				// in other cases we consider that the region client is healthy
				// and return the error to caller to let them retry
			}
		}
	}
}

func (c *client) receive() (err error) {
	var (
		sz       [4]byte
		header   pb.ResponseHeader
		response proto.Message
	)

	err = c.readFully(sz[:])
	if err != nil {
		return ServerError{err}
	}

	size := binary.BigEndian.Uint32(sz[:])
	b := make([]byte, size)

	err = c.readFully(b)
	if err != nil {
		return ServerError{err}
	}

	// unmarshal header
	headerBytes, headerLen := protowire.ConsumeBytes(b)
	if headerLen < 0 {
		return ServerError{fmt.Errorf("failed to decode the response header: %v",
			protowire.ParseError(headerLen))}
	}
	if err = proto.Unmarshal(headerBytes, &header); err != nil {
		return ServerError{fmt.Errorf("failed to decode the response header: %v", err)}
	}

	if header.CallId == nil {
		return ErrMissingCallID
	}

	callID := *header.CallId
	rpc := c.unregisterRPC(callID)
	if rpc == nil {
		return ServerError{fmt.Errorf("got a response with an unexpected call ID: %d", callID)}
	}
	if err := c.inFlightDown(); err != nil {
		return ServerError{err}
	}

	select {
	case <-rpc.Context().Done():
		// context has expired, don't bother deserializing
		return
	default:
	}

	// Here we know for sure that we got a response for rpc we asked.
	// It's our responsibility to deliver the response or error to the
	// caller as we unregistered the rpc.
	defer func() { returnResult(rpc, response, err) }()

	if header.Exception != nil {
		err = exceptionToError(*header.Exception.ExceptionClassName, *header.Exception.StackTrace)
		return
	}

	response = rpc.NewResponse()

	responseBytes, responseLen := protowire.ConsumeBytes(b[headerLen:])
	if responseLen < 0 {
		err = RetryableError{fmt.Errorf("failed to decode the response: %s",
			protowire.ParseError(responseLen))}
		return
	}

	if err = proto.Unmarshal(responseBytes, response); err != nil {
		err = RetryableError{fmt.Errorf("failed to decode the response: %s", err)}
		return
	}

	var cellsLen uint32
	if header.CellBlockMeta != nil {
		cellsLen = header.CellBlockMeta.GetLength()
	}
	if d, ok := rpc.(canDeserializeCellBlocks); cellsLen > 0 && ok {
		b := b[size-cellsLen:]
		if c.compressor != nil {
			b, err = c.compressor.decompressCellblocks(b)
			if err != nil {
				err = RetryableError{fmt.Errorf("failed to decompress the response: %s", err)}
				return
			}
		}
		var nread uint32
		nread, err = d.DeserializeCellBlocks(response, b)
		if err != nil {
			err = RetryableError{fmt.Errorf("failed to decode the response: %s", err)}
			return
		}

		if int(nread) < len(b) {
			err = RetryableError{
				fmt.Errorf("short read: buffer length %d, read %d", len(b), nread)}
			return
		}
	}
	return
}

func exceptionToError(class, stack string) error {
	err := fmt.Errorf("HBase Java exception %s:\n%s", class, stack)
	if s, ok := javaRetryableExceptions[class]; ok && strings.Contains(stack, s) {
		return RetryableError{err}
	} else if s, ok := javaRegionExceptions[class]; ok && strings.Contains(stack, s) {
		return NotServingRegionError{err}
	} else if s, ok := javaServerExceptions[class]; ok && strings.Contains(stack, s) {
		return ServerError{err}
	}
	return err
}

// write sends the given buffer to the RegionServer.
func (c *client) write(buf []byte) error {
	_, err := c.conn.Write(buf)
	return err
}

// Tries to read enough data to fully fill up the given buffer.
func (c *client) readFully(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	return err
}

// sendHello sends the "hello" message needed when opening a new connection.
func (c *client) sendHello() error {
	connHeader := &pb.ConnectionHeader{
		UserInfo: &pb.UserInformation{
			EffectiveUser: proto.String(c.effectiveUser),
		},
		ServiceName:         proto.String(string(c.ctype)),
		CellBlockCodecClass: proto.String("org.apache.hadoop.hbase.codec.KeyValueCodec"),
	}
	if c.compressor != nil {
		// if we have compression enabled, specify the compressor class
		connHeader.CellBlockCompressorClass = proto.String(c.compressor.CellBlockCompressorClass())
	}
	data, err := proto.Marshal(connHeader)
	if err != nil {
		return fmt.Errorf("failed to marshal connection header: %s", err)
	}

	const header = "HBas\x00\x50" // \x50 = Simple Auth.
	buf := make([]byte, 0, len(header)+4+len(data))
	buf = append(buf, header...)
	buf = buf[:len(header)+4]
	binary.BigEndian.PutUint32(buf[6:], uint32(len(data)))
	buf = append(buf, data...)
	return c.write(buf)
}

// send sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *client) send(rpc hrpc.Call) (uint32, error) {
	var err error
	var request proto.Message
	var cellblocks net.Buffers
	var cellblocksLen uint32
	header := &pb.RequestHeader{
		MethodName:   proto.String(rpc.Name()),
		RequestParam: proto.Bool(true),
	}

	if s, ok := rpc.(canSerializeCellBlocks); ok && s.CellBlocksEnabled() {
		// request can be serialized to cellblocks
		request, cellblocks, cellblocksLen = s.SerializeCellBlocks()

		if c.compressor != nil {
			// we have compressor, encode the cellblocks
			compressed := c.compressor.compressCellblocks(cellblocks, cellblocksLen)
			defer freeBuffer(compressed)
			cellblocks = net.Buffers{compressed}
			cellblocksLen = uint32(len(compressed))
		}

		// specify cellblocks length
		header.CellBlockMeta = &pb.CellBlockMeta{
			Length: &cellblocksLen,
		}
	} else {
		// plain protobuf request
		request = rpc.ToProto()
	}

	// we have to register rpc after we marshal because
	// registered rpc can fail before it was even sent
	// in all the cases where c.fail() is called.
	// If that happens, client can retry sending the rpc
	// again potentially changing it's contents.
	id := c.registerRPC(rpc)
	header.CallId = &id

	b := newBuffer(4)
	defer func() { freeBuffer(b) }()

	b = protowire.AppendVarint(b, uint64(proto.Size(header)))
	b, err = proto.MarshalOptions{}.MarshalAppend(b, header)
	if err != nil {
		return id, fmt.Errorf("failed to marshal request header: %s", err)
	}

	if request == nil {
		return id, errors.New("failed to marshal request: proto: Marshal called with nil")
	}
	b = protowire.AppendVarint(b, uint64(proto.Size(request)))
	b, err = proto.MarshalOptions{}.MarshalAppend(b, request)
	if err != nil {
		return id, fmt.Errorf("failed to marshal request: %s", err)
	}

	binary.BigEndian.PutUint32(b, uint32(len(b))+cellblocksLen-4)

	if cellblocks != nil {
		bfs := append(net.Buffers{b}, cellblocks...)
		_, err = bfs.WriteTo(c.conn)
	} else {
		err = c.write(b)
	}
	if err != nil {
		return id, ServerError{err}
	}

	if err := c.inFlightUp(); err != nil {
		return id, ServerError{err}
	}
	return id, nil
}
