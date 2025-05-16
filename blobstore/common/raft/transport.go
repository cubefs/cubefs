// Copyright 2022 The CubeFS Authors.
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

package raft

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
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
	raftSendBufferSize     = 10000
	defaultInflightMsgSize = 4 << 10
	defaultDialTimeout     = 200 * time.Millisecond
	defaultNetwork         = "tcp"
)

const (
	messageUrl  = "/raft/message/batch"
	snapshotUrl = "/raft/snapshot"
)

type (
	transportHandler interface {
		UniqueID() uint64
		// HandleRaftRequest handle incoming raft request
		HandleRaftRequest(ctx context.Context, req *RaftMessageRequest, stream MessageResponseStream) error
		// HandleRaftResponse handle with raft response error
		HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error
		// HandleRaftSnapshot handle with raft snapshot request
		HandleRaftSnapshot(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error
	}

	TransportConfig struct {
		MaxInflightMsgSize int          `json:"max_inflight_msg_size"`
		Addr               string       `json:"addr"`
		Network            string       `json:"network"`
		Client             rpc2.Client  `json:"rpc2_client"`
		Server             *rpc2.Server `json:"rpc2_server"`

		Resolver AddressResolver `json:"-"`
	}
)

func NewTransport(cfg *TransportConfig) *Transport {
	defaulter.Empty(&cfg.Network, defaultNetwork)
	defaulter.Empty(&cfg.Client.ConnectorConfig.Network, cfg.Network)
	defaulter.IntegerLessOrEqual(&cfg.Client.ConnectorConfig.DialTimeout.Duration, defaultDialTimeout)
	defaulter.LessOrEqual(&cfg.MaxInflightMsgSize, defaultInflightMsgSize)
	t := &Transport{
		resolver: cfg.Resolver,
		cfg:      cfg,
		done:     make(chan struct{}),
	}
	t.newServer()
	return t
}

type Transport struct {
	queues   [numConnectionClasses]sync.Map
	resolver AddressResolver
	handlers sync.Map
	server   *rpc2.Server
	clients  sync.Map

	cfg  *TransportConfig
	done chan struct{}
}

func (t *Transport) RaftMessageBatch(resp rpc2.ResponseWriter, req *rpc2.Request) error {
	var (
		span  trace.Span
		errCh = make(chan error, 1)
	)
	stream := &rpc2.GenericServerStream[RaftMessageRequestBatch, RaftMessageResponse]{ServerStream: req.ServerStream()}
	err := stream.SendHeader(nil)
	if err != nil {
		return err
	}
	go func(ctx context.Context) {
		span, ctx = trace.StartSpanFromContext(ctx, "")
		errCh <- func() error {
			for {
				/*start := time.Now()*/
				batch, err := stream.Recv()
				if err != nil {
					return err
				}
				/*recvCost := time.Since(start)
				start = time.Now()*/

				if len(batch.Requests) == 0 {
					continue
				}

				for i := range batch.Requests {
					req := &batch.Requests[i]
					// dispatch manager by request.To
					handler, ok := t.handlers.Load(req.UniqueID())
					if !ok {
						continue
					}
					if err := handler.(transportHandler).HandleRaftRequest(ctx, req, stream); err != nil {
						span.Errorf("handle raft request[%+v] failed: %s", req, err)
						if err := stream.Send(newRaftMessageResponse(req, ErrGroupHandleRaftMessage)); err != nil {
							return err
						}
					}

					/*if req.IsCoalescedHeartbeat() {
						handleCost := time.Since(start)
						span.Debugf("handle raft batch request[%d], heartbeat num: %d, heartbeat resp num: %d, receive cost: %dus, handle cost: %dus",
							len(batch.Requests), len(req.Heartbeats), len(req.HeartbeatResponses), recvCost/time.Microsecond, handleCost/time.Microsecond)
					}*/
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

func (t *Transport) RaftSnapshot(resp rpc2.ResponseWriter, req *rpc2.Request) error {
	var (
		span  trace.Span
		errCh = make(chan error, 1)
	)
	stream := &rpc2.GenericServerStream[RaftSnapshotRequest, RaftSnapshotResponse]{ServerStream: req.ServerStream()}
	err := stream.SendHeader(nil)
	if err != nil {
		return err
	}
	go func(ctx context.Context) {
		span, ctx = trace.StartSpanFromContext(ctx, "")
		errCh <- func() error {
			rs, err := stream.Recv()
			if err != nil {
				return err
			}
			if rs.Header == nil {
				return stream.Send(&RaftSnapshotResponse{
					Status:  RaftSnapshotResponse_ERROR,
					Message: "snapshot sender error: no header in the first snapshot request",
				})
			}

			// dispatch manager by req.Header.Req.To
			handler, ok := t.handlers.Load(rs.Header.RaftMessageRequest.UniqueID())
			if !ok {
				return fmt.Errorf("can't find handler by: %+v", req.Header)
			}
			if err = handler.(transportHandler).HandleRaftSnapshot(ctx, rs, stream); err != nil {
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
func (t *Transport) SendAsync(ctx context.Context, req *RaftMessageRequest, class connectionClass) error {
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

func (t *Transport) SendSnapshot(ctx context.Context, snapshot *outgoingSnapshot, req *RaftMessageRequest) error {
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
			Members: snapshot.Members(),
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

// RegisterHandler register handler
func (t *Transport) RegisterHandler(h transportHandler) {
	t.handlers.Store(h.UniqueID(), h)
}

func (t *Transport) Close() {
	close(t.done)
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(200*time.Millisecond))
	t.server.Shutdown(ctx)
	cancelFunc()
}

func (t *Transport) getSnapshotStream(ctx context.Context, to uint64) (SnapshotClient, error) {
	addr, err := t.resolver.Resolve(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("can't resolve to node id[%d]", to)
	}
	span := trace.SpanFromContextSafe(ctx)
	snapshotStreamClient := t.getSnapshotStreamClient()
	req, err := rpc2.NewStreamRequest(ctx, addr.String(), snapshotUrl, nil)
	if err != nil {
		span.Warnf("new snapshot stream request for node[%d] failed: %s", to, err)
		return nil, err
	}
	streamingCli, err := snapshotStreamClient.Streaming(req, nil)
	if err != nil {
		span.Warnf("get snapshot stream for node[%d] failed: %s", to, err)
		return nil, err
	}
	stream := &rpc2.GenericClientStream[RaftSnapshotRequest, RaftSnapshotResponse]{ClientStream: streamingCli}
	return stream, nil
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *Transport) getQueue(
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
func (t *Transport) startProcessNewQueue(
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

	messageStreamClient := t.getMessageStreamClient(class)
	req, err := rpc2.NewStreamRequest(ctx, addr, messageUrl, nil)
	if err != nil {
		span.Warnf("new stream request for node[%d] failed: %s", toNodeID, err)
		return
	}
	streamingCli, err := messageStreamClient.Streaming(req, nil)
	if err != nil {
		span.Warnf("get stream for node[%d] failed: %s", toNodeID, err)
		return
	}
	stream := &rpc2.GenericClientStream[RaftMessageRequestBatch, RaftMessageResponse]{ClientStream: streamingCli}
	if err := t.processQueue(ch, stream); err != nil {
		span.Warnf("processing raft message queue for node[%d] failed: %s", toNodeID, err)
	}
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *Transport) processQueue(
	ch chan *RaftMessageRequest,
	stream MessageClient,
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

				handler, _ := t.handlers.Load(resp.UniqueID())
				if err := handler.(transportHandler).HandleRaftResponse(ctx, resp); err != nil {
					return err
				}
			}
		}()
	}(ctx)

	batch := &RaftMessageRequestBatch{}
	for {
		select {
		case err := <-errCh:
			span.Errorf("Transport.processQueue failed: %s", err)
			return err
		case req := <-ch:
			/*isHeartbeatReq := false
			heartbeatReqIndex := 0*/
			budget := t.cfg.MaxInflightMsgSize
			/*if req.IsCoalescedHeartbeat() {
				isHeartbeatReq = true
			}
			start := time.Now()*/

			batch.Requests = append(batch.Requests, *req)
			req.Release()
			// pull off as many queued requests as possible
		BUDGET:
			for budget > 0 {
				select {
				case req = <-ch:
					/*					if req.IsCoalescedHeartbeat() {
										isHeartbeatReq = true
										heartbeatReqIndex = len(batch.Requests)
									}*/

					budget -= req.Size()
					batch.Requests = append(batch.Requests, *req)
					req.Release()
				default:
					break BUDGET
				}
			}
			/*budgetCost := time.Since(start)
			start = time.Now()*/

			err := stream.Send(batch)
			if err != nil {
				return err
			}
			/*sendCost := time.Since(start)*/

			/*if isHeartbeatReq {
				span.Debugf("send raft batch request[%d], heartbeat num: %d, heartbeat resp num: %d, budget cost: %dus, send cost: %dus",
					len(batch.Requests), len(batch.Requests[heartbeatReqIndex].Heartbeats), len(batch.Requests[heartbeatReqIndex].HeartbeatResponses),
					budgetCost/time.Microsecond, sendCost/time.Microsecond)
			}*/

			// reuse the Requests slice, zero out the contents to avoid delaying
			// GC of memory referenced from within.
			for i := range batch.Requests {
				// recycle heartbeat slice
				if batch.Requests[i].IsCoalescedHeartbeat() {
					if len(batch.Requests[i].Heartbeats) > 0 {
						s := batch.Requests[i].Heartbeats[:0]
						raftHeartbeatPool.Put(&s)
					}
					if len(batch.Requests[i].HeartbeatResponses) > 0 {
						s := batch.Requests[i].HeartbeatResponses[:0]
						raftHeartbeatPool.Put(&s)
					}
				}
				batch.Requests[i] = RaftMessageRequest{}
			}
			batch.Requests = batch.Requests[:0]
		}
	}
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

func (t *Transport) newServer() {
	handler := &rpc2.Router{}
	handler.Register(snapshotUrl, t.RaftSnapshot)
	handler.Register(messageUrl, t.RaftMessageBatch)

	if t.cfg.Server == nil {
		t.cfg.Server = &rpc2.Server{}
	}
	defaulter.Empty(&t.cfg.Server.Name, t.cfg.Addr)
	t.cfg.Server.Handler = handler.MakeHandler()
	if len(t.cfg.Server.Addresses) == 0 {
		t.cfg.Server.Addresses = []rpc2.NetworkAddress{{Network: t.cfg.Network, Address: t.cfg.Addr}}
	}

	server := t.cfg.Server
	server.RegisterOnShutdown(func() { log.Info("shutdown") })
	go func() {
		if err := server.Serve(); err != nil && err != rpc2.ErrServerClosed {
			panic(err)
		}
	}()
	t.server = t.cfg.Server
}

func (t *Transport) getSnapshotStreamClient() *rpc2.StreamClient[RaftSnapshotRequest, RaftSnapshotResponse] {
	return &rpc2.StreamClient[RaftSnapshotRequest, RaftSnapshotResponse]{Client: t.getClient(defaultConnectionClass1)}
}

func (t *Transport) getMessageStreamClient(class connectionClass) *rpc2.StreamClient[RaftMessageRequestBatch, RaftMessageResponse] {
	return &rpc2.StreamClient[RaftMessageRequestBatch, RaftMessageResponse]{Client: t.getClient(class)}
}

func (t *Transport) getClient(class connectionClass) *rpc2.Client {
	newClient := t.cfg.Client
	value, _ := t.clients.LoadOrStore(class, &newClient)
	return value.(*rpc2.Client)
}

// SnapshotClient is the subset of the interface that is needed for sending snapshot requests.
type SnapshotClient interface {
	Send(*RaftSnapshotRequest) error
	Recv() (*RaftSnapshotResponse, error)
	rpc2.ClientStream
}

// MessageClient is the subset of the interface that is needed for sending batch message requests.
type MessageClient interface {
	Send(*RaftMessageRequestBatch) error
	Recv() (*RaftMessageResponse, error)
	rpc2.ClientStream
}
