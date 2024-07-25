package raft

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type connectionClass int8

const (
	// defaultConnectionClass is the default ConnectionClass and should be used for most
	// client traffic.
	defaultConnectionClass1 connectionClass = iota + 1
	defaultConnectionClass2
	defaultConnectionClass3
	// systemConnectionClass is the ConnectionClass used for system traffic like heartbeat.
	systemConnectionClass
	// numConnectionClasses is the number of valid connectionClass values.
	numConnectionClasses
)

const (
	raftSendBufferSize = 10000

	defaultInflightMsgSize         = 4 << 10
	defaultConnectionTimeoutMs     = uint32(100)
	defaultMaxTimeoutMs            = uint32(5000)
	defaultBackoffMaxDelayMs       = uint32(5000)
	defaultBackoffBaseDelayMs      = uint32(200)
	defaultKeepAliveTimeoutS       = uint32(15)
	defaultServerKeepAliveTimeoutS = uint32(10)
)

type (
	transportHandler interface {
		// HandleRaftRequest handle incoming raft request
		HandleRaftRequest(ctx context.Context, req *RaftMessageRequest, stream MessageResponseStream) error
		// HandleRaftResponse handle with raft response error
		HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error
		// HandleRaftSnapshot handle with raft snapshot request
		HandleRaftSnapshot(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error
	}

	TransportConfig struct {
		MaxInflightMsgSize      int    `json:"max_inflight_msg_size"`
		MaxTimeoutMs            uint32 `json:"max_timeout_ms"`
		ConnectTimeoutMs        uint32 `json:"connect_timeout_ms"`
		KeepaliveTimeoutS       uint32 `json:"keepalive_timeout_s"`
		ServerKeepaliveTimeoutS uint32 `json:"server_keepalive_timeout_s"`
		BackoffBaseDelayMs      uint32 `json:"backoff_base_delay_ms"`
		BackoffMaxDelayMs       uint32 `json:"backoff_max_delay_ms"`
		Addr                    string `json:"addr"`

		Resolver AddressResolver  `json:"-"`
		Handler  transportHandler `json:"-"`
	}
)

func newTransport(cfg *TransportConfig) *transport {
	initialDefaultConfig(&cfg.MaxInflightMsgSize, defaultInflightMsgSize)
	initialDefaultConfig(&cfg.ConnectTimeoutMs, defaultConnectionTimeoutMs)
	initialDefaultConfig(&cfg.MaxTimeoutMs, defaultMaxTimeoutMs)
	initialDefaultConfig(&cfg.KeepaliveTimeoutS, defaultKeepAliveTimeoutS)
	initialDefaultConfig(&cfg.ServerKeepaliveTimeoutS, defaultServerKeepAliveTimeoutS)
	initialDefaultConfig(&cfg.BackoffBaseDelayMs, defaultBackoffBaseDelayMs)
	initialDefaultConfig(&cfg.BackoffMaxDelayMs, defaultBackoffMaxDelayMs)
	if cfg.ServerKeepaliveTimeoutS > cfg.KeepaliveTimeoutS {
		log.Fatalf("server keepalive timeout must less than client keepalive timeout")
	}

	t := &transport{
		resolver: cfg.Resolver,
		handler:  cfg.Handler,
		server:   grpc.NewServer(generateServerOpts(cfg)...),
		cfg:      cfg,
		done:     make(chan struct{}),
	}

	// register raft service

	RegisterRaftServiceServer(t.server, t)

	// start grpc server
	Listener, err := net.Listen("tcp", t.cfg.Addr)
	if err != nil {
		log.Fatalf("listen addr[%s] failed: %s", cfg.Addr, err)
	}
	go t.server.Serve(Listener)

	return t
}

type transport struct {
	queues   [numConnectionClasses]sync.Map
	resolver AddressResolver
	handler  transportHandler
	server   *grpc.Server
	conns    sync.Map

	cfg  *TransportConfig
	done chan struct{}
}

func (t *transport) RaftMessageBatch(stream RaftService_RaftMessageBatchServer) error {
	var (
		span  trace.Span
		errCh = make(chan error, 1)
	)

	go func(ctx context.Context) {
		span, ctx = trace.StartSpanFromContext(ctx, "")
		errCh <- func() error {
			stream := &lockedMessageResponseStream{wrapped: stream}
			for {
				start := time.Now()
				batch, err := stream.Recv()
				if err != nil {
					return err
				}
				recvCost := time.Since(start)
				start = time.Now()

				if len(batch.Requests) == 0 {
					continue
				}

				for i := range batch.Requests {
					req := &batch.Requests[i]
					if err := t.handler.HandleRaftRequest(ctx, req, stream); err != nil {
						span.Errorf("handle raft request[%+v] failed: %s", req, err)
						if err := stream.Send(newRaftMessageResponse(req, ErrGroupHandleRaftMessage)); err != nil {
							return err
						}
					}

					if req.IsCoalescedHeartbeat() {
						handleCost := time.Since(start)
						span.Infof("handle raft batch request[%d], heartbeat num: %d, heartbeat resp num: %d, receive cost: %dus, handle cost: %dus",
							len(batch.Requests), len(req.Heartbeats), len(req.HeartbeatResponses), recvCost/time.Microsecond, handleCost/time.Microsecond)
					}
				}
			}
		}()
	}(stream.Context())

	select {
	case <-t.done:
		return nil
	case err := <-errCh:
		span.Errorf("service handle RaftMessageBatch failed: %s", err)
		return err
	}
}

func (t *transport) RaftSnapshot(stream RaftService_RaftSnapshotServer) error {
	var (
		span  trace.Span
		errCh = make(chan error, 1)
	)

	go func(ctx context.Context) {
		span, ctx = trace.StartSpanFromContext(ctx, "")

		errCh <- func() error {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			if req.Header == nil {
				return stream.Send(&RaftSnapshotResponse{
					Status:  RaftSnapshotResponse_ERROR,
					Message: "snapshot sender error: no header in the first snapshot request",
				})
			}

			if err = t.handler.HandleRaftSnapshot(ctx, req, stream); err != nil {
				span.Errorf("handle raft snapshot failed: %s", err)
			}
			return err
		}()
	}(stream.Context())

	select {
	case <-t.done:
		return nil
	case err := <-errCh:
		if err != nil {
			span.Errorf("service handle RaftSnapshot failed: %s", err)
		}
		return err
	}
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *transport) SendAsync(ctx context.Context, req *RaftMessageRequest, class connectionClass) error {
	// span := trace.SpanFromContext(ctx)

	toNodeID := req.To
	if req.GroupID == 0 && len(req.Heartbeats) == 0 && len(req.HeartbeatResponses) == 0 {
		// Coalesced heartbeats are addressed to range 0; everything else
		// needs an explicit range ID.
		panic("only messages with coalesced heartbeats or heartbeat responses may be sent to range ID 0")
	}

	// span.Infof("send async request: %+v", req)
	// resolve address from to node id
	addr, err := t.resolver.Resolve(ctx, toNodeID)
	if err != nil {
		return fmt.Errorf("can't resolve to node id[%d], err: %s", toNodeID, err)
	}

	ch, existingQueue := t.getQueue(addr.String(), class)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		_, ctx := trace.StartSpanFromContext(context.Background(), "")
		go t.startProcessNewQueue(ctx, toNodeID, addr.String(), class)
	}

	select {
	case ch <- req:
		return nil
	default:
		return fmt.Errorf("send request into group queue failed, queue is full")
	}
}

func (t *transport) SendSnapshot(ctx context.Context, snapshot *outgoingSnapshot, req *RaftMessageRequest) error {
	span := trace.SpanFromContext(ctx)

	stream, err := t.getSnapshotStream(ctx, req.To)
	if err != nil {
		return errors.Info(err, "get snapshot stream failed")
	}

	defer func() {
		if err = stream.CloseSend(); err != nil {
			span.Warnf("failed to close outgoingSnapshot stream: %s", err)
		}
	}()

	// send header first
	snapshotReq := &RaftSnapshotRequest{
		Header: &RaftSnapshotHeader{
			ID: string(req.Message.Snapshot.Data),
			RaftMessageRequest: &RaftMessageRequest{
				GroupID: req.GroupID,
				From:    req.From,
				To:      req.To,
				Message: req.Message,
			},
		},
	}
	if err = stream.Send(snapshotReq); err != nil {
		return errors.Info(err, "send raft snapshot request failed")
	}

	snapshotReq.Header = nil
	snapshotReq.Seq++

	// then receive response to check accepted or not
	resp, err := stream.Recv()
	if err != nil {
		return errors.Info(err, "receive first response failed")
	}
	if resp.Status != RaftSnapshotResponse_ACCEPTED {
		return fmt.Errorf("unexpected snapshot response status[%d], message: %s", resp.Status, resp.Message)
	}

	for !snapshotReq.Final {
		var batch Batch
		batch, err = snapshot.BatchData()
		if err != nil {
			if err != io.EOF {
				return err
			}
			snapshotReq.Final = true
		}

		if batch != nil {
			snapshotReq.Data = batch.Data()
		}
		if err = stream.Send(snapshotReq); err != nil {
			if batch != nil {
				batch.Close()
			}
			return errors.Info(err, "send snapshot data failed", snapshotReq)
		}

		if batch != nil {
			batch.Close()
		}
		snapshotReq.Data = nil
		snapshotReq.Seq++
	}

	// receive response to check finish or not
	resp, err = stream.Recv()
	if err != nil {
		return errors.Info(err, "receive last response failed")
	}
	if resp.Status != RaftSnapshotResponse_APPLIED {
		return fmt.Errorf("unexpected snapshot response status[%d], message: %s", resp.Status, resp.Message)
	}

	return nil
}

func (t *transport) getSnapshotStream(ctx context.Context, to uint64) (RaftService_RaftSnapshotClient, error) {
	addr, err := t.resolver.Resolve(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("can't resolve to node id[%d]", to)
	}

	conn, err := t.getConnection(ctx, addr.String(), defaultConnectionClass1)
	if err != nil {
		return nil, err
	}

	client := NewRaftServiceClient(conn.ClientConn)
	stream, err := client.RaftSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (t *transport) Close() {
	close(t.done)
	t.server.GracefulStop()
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *transport) getQueue(
	addr string, class connectionClass,
) (chan *RaftMessageRequest, bool) {
	queuesMap := &t.queues[class]
	value, ok := queuesMap.Load(addr)
	if !ok {
		ch := make(chan *RaftMessageRequest, raftSendBufferSize)
		value, ok = queuesMap.LoadOrStore(addr, ch)
	}
	return value.(chan *RaftMessageRequest), ok
}

// startProcessNewQueue connects to the node and launches a worker goroutine
// that processes the queue for the given toNodeID (which must exist) until
// the underlying connection is closed or an error occurs. This method
// takes on the responsibility of deleting the queue when the worker shuts down.
// The class parameter dictates the ConnectionClass which should be used to dial
// the remote node. Traffic for system ranges and heartbeats will receive a
// different class than that of user data ranges.
//
// Returns whether the worker was started (the queue is deleted either way).
func (t *transport) startProcessNewQueue(
	ctx context.Context,
	toNodeID uint64,
	addr string,
	class connectionClass,
) {
	span := trace.SpanFromContext(ctx)

	ch, existingQueue := t.getQueue(addr, class)
	if !existingQueue {
		span.Fatalf("queue[%s] does not exist", addr)
	}
	defer t.queues[class].Delete(addr)

	conn, err := t.getConnection(ctx, addr, class)
	if err != nil {
		span.Warnf("get connection for node[%d] failed: %s", toNodeID, err)
		return
	}
	client := NewRaftServiceClient(conn.ClientConn)
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.RaftMessageBatch(batchCtx) // closed via cancellation
	if err != nil {
		// shall we need to remove error connection?
		// Note: as the gRPC ClientConn will retry backoff to reconnection,
		// there's no need to create new connection
		span.Warnf("create raft message batch client for node[%d] failed: %s", toNodeID, err)
		return
	}

	if err := t.processQueue(ch, stream); err != nil {
		span.Warnf("processing raft message queue for node[%d] failed: %s", toNodeID, err)
	}
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *transport) processQueue(
	ch chan *RaftMessageRequest,
	stream RaftService_RaftMessageBatchClient,
) error {
	var (
		ctx   = stream.Context()
		span  = trace.SpanFromContext(ctx)
		errCh = make(chan error, 1)
	)

	go func(ctx context.Context) {
		errCh <- func() error {
			for {
				resp, err := stream.Recv()
				if err != nil {
					return err
				}

				if err := t.handler.HandleRaftResponse(ctx, resp); err != nil {
					return err
				}
			}
		}()
	}(ctx)

	batch := &RaftMessageRequestBatch{}
	for {
		select {
		case err := <-errCh:
			span.Errorf("transport.processQueue failed: %s", err)
			return err
		case req := <-ch:
			isHeartbeatReq := false
			heartbeatReqIndex := 0
			budget := t.cfg.MaxInflightMsgSize
			if req.IsCoalescedHeartbeat() {
				isHeartbeatReq = true
			}
			start := time.Now()

			batch.Requests = append(batch.Requests, *req)
			req.Release()
			// pull off as many queued requests as possible
			for budget > 0 {
				select {
				case req = <-ch:
					if req.IsCoalescedHeartbeat() {
						isHeartbeatReq = true
						heartbeatReqIndex = len(batch.Requests)
					}

					budget -= req.Size()
					batch.Requests = append(batch.Requests, *req)
					req.Release()
				default:
					budget = -1
				}
			}
			budgetCost := time.Since(start)
			start = time.Now()

			err := stream.Send(batch)
			if err != nil {
				return err
			}
			sendCost := time.Since(start)

			if isHeartbeatReq {
				span.Infof("send raft batch request[%d], heartbeat num: %d, heartbeat resp num: %d, budget cost: %dus, send cost: %dus",
					len(batch.Requests), len(batch.Requests[heartbeatReqIndex].Heartbeats), len(batch.Requests[heartbeatReqIndex].HeartbeatResponses),
					budgetCost/time.Microsecond, sendCost/time.Microsecond)
			}

			// reuse the Requests slice, zero out the contents to avoid delaying
			// GC of memory referenced from within.
			for i := range batch.Requests {
				// recycle heartbeat slice
				if batch.Requests[i].IsCoalescedHeartbeat() {
					var s []RaftHeartbeat
					if len(batch.Requests[i].Heartbeats) > 0 {
						s = batch.Requests[i].Heartbeats[:0]
					} else {
						s = batch.Requests[i].HeartbeatResponses[:0]
					}
					raftHeartbeatPool.Put(&s)
				}
				batch.Requests[i] = RaftMessageRequest{}
			}
			batch.Requests = batch.Requests[:0]
		}
	}
}

type connectionKey struct {
	target string
	class  connectionClass
}

func (t *transport) getConnection(ctx context.Context, target string, class connectionClass) (conn *connection, err error) {
	key := connectionKey{
		target: target,
		class:  class,
	}

	value, loaded := t.conns.Load(key)
	if !loaded {
		value, _ = t.conns.LoadOrStore(key, &connection{})
	}
	conn = value.(*connection)

	if conn.ClientConn == nil {
		conn.once.Do(func() {
			grpcConn, dialErr := grpc.DialContext(ctx, target, generateDialOpts(t.cfg)...)
			if dialErr != nil {
				err = dialErr
				t.conns.Delete(target)
				return
			}
			grpcConn.Connect()
			conn.ClientConn = grpcConn
		})
	}

	return
}

type connection struct {
	*grpc.ClientConn

	once sync.Once
}

// MessageResponseStream is the subset of the
// RaftService_RaftMessageBatchServer interface that is needed for sending responses.
type MessageResponseStream interface {
	Context() context.Context
	Send(*RaftMessageResponse) error
}

// SnapshotResponseStream is the subset of the
// RaftService_RaftSnapshotServer interface that is needed for sending responses.
type SnapshotResponseStream interface {
	Context() context.Context
	Send(response *RaftSnapshotResponse) error
	Recv() (*RaftSnapshotRequest, error)
}

// lockedMessageResponseStream is an implementation of
// MessageResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedMessageResponseStream struct {
	wrapped RaftService_RaftMessageBatchServer
	sendMu  sync.Mutex
}

func (s *lockedMessageResponseStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedMessageResponseStream) Send(resp *RaftMessageResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(resp)
}

func (s *lockedMessageResponseStream) Recv() (*RaftMessageRequestBatch, error) {
	return s.wrapped.Recv()
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(req *RaftMessageRequest, err *Error) *RaftMessageResponse {
	resp := &RaftMessageResponse{
		GroupID: req.GroupID,
		To:      req.From,
		From:    req.To,
	}
	if err != nil {
		resp.Err = err
	}
	return resp
}

// generateDialOpts generate grpc dial options
func generateDialOpts(cfg *TransportConfig) []grpc.DialOption {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Timeout:             time.Duration(cfg.KeepaliveTimeoutS) * time.Second,
				PermitWithoutStream: true,
			},
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay: time.Duration(cfg.BackoffBaseDelayMs) * time.Millisecond,
				MaxDelay:  time.Duration(cfg.BackoffMaxDelayMs) * time.Millisecond,
			},
			MinConnectTimeout: time.Millisecond * time.Duration(cfg.ConnectTimeoutMs),
		}),
		grpc.WithChainStreamInterceptor(unaryInterceptorWithTracer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return dialOpts
}

func generateServerOpts(cfg *TransportConfig) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Duration(cfg.ServerKeepaliveTimeoutS) * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(serverUnaryInterceptorWithTracer),
	}
}

// unaryInterceptorWithTracer intercept client request with trace id
/*func unaryInterceptorWithTracer(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	span := trace.SpanFromContextSafe(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		reqIDKey, span.TraceID(),
	))

	return invoker(ctx, method, req, reply, cc, opts...)
}*/

// unaryInterceptorWithTracer intercept client request with trace id
func unaryInterceptorWithTracer(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	span := trace.SpanFromContextSafe(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		reqIDKey, span.TraceID(),
	))

	return streamer(ctx, desc, cc, method, opts...)
}

/*// serverUnaryInterceptorWithTracer intercept incoming request with trace id
func serverUnaryInterceptorWithTracer_(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "failed to get metadata")
	}
	reqId, ok := md[reqIDKey]
	if ok {
		_, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", reqId[0])
	} else {
		_, ctx = trace.StartSpanFromContext(ctx, "")
	}

	resp, err = handler(ctx, req)
	return
}*/

func serverUnaryInterceptorWithTracer(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "failed to get metadata")
	}
	reqId, ok := md[reqIDKey]
	if ok {
		_, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", reqId[0])
	} else {
		_, ctx = trace.StartSpanFromContext(ctx, "")
	}
	ctx.Err()

	return handler(srv, ss)
}
