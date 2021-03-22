// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test"
	"github.com/tsuna/gohbase/test/mock"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestErrors(t *testing.T) {
	ue := ServerError{fmt.Errorf("oops")}
	if ue.Error() != "region.ServerError: oops" {
		t.Errorf("Wrong error message. Got %q, wanted %q", ue, "region.ServerError: oops")
	}
}

func TestWrite(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn: mockConn,
	}

	// check if Write returns an error
	expectErr := errors.New("nope")
	mockConn.EXPECT().Write(gomock.Any()).Return(0, expectErr).Times(1)
	err := c.write([]byte("lol"))
	if err != expectErr {
		t.Errorf("expected %v, got %v", expectErr, err)
	}

	// check if it actually writes the right data
	expected := []byte("lol")
	mockConn.EXPECT().Write(gomock.Any()).Return(3, nil).Times(1).Do(func(buf []byte) {
		if !bytes.Equal(expected, buf) {
			t.Errorf("expected %v, got %v", expected, buf)
		}
	})
	err = c.write(expected)
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}
}

func TestSendHello(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		effectiveUser: "root",
		ctype:         RegionClient,
	}

	// check if it's sending the right "hello" for RegionClient
	mockConn.EXPECT().Write(gomock.Any()).Return(78, nil).Times(1).Do(func(buf []byte) {
		expected := []byte("HBas\x00P\x00\x00\x00D\n\x06\n\x04root\x12\rClientService\x1a+" +
			"org.apache.hadoop.hbase.codec.KeyValueCodec")
		if !bytes.Equal(expected, buf) {
			t.Errorf("expected %v, got %v", expected, buf)
		}
	})
	err := c.sendHello()
	if err != nil {
		t.Errorf("Wasn't expecting error, but got one: %#v", err)
	}

	// check if it sends the right "hello" for MasterClient
	c.ctype = MasterClient
	mockConn.EXPECT().Write(gomock.Any()).Return(78, nil).Times(1).Do(func(buf []byte) {
		expected := []byte("HBas\x00P\x00\x00\x00D\n\x06\n\x04root\x12\rMasterService\x1a+" +
			"org.apache.hadoop.hbase.codec.KeyValueCodec")
		if !bytes.Equal(expected, buf) {
			t.Errorf("expected %v, got %v", expected, buf)
		}
	})
	err = c.sendHello()
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}

	c.compressor = &compressor{Codec: mockCodec{}}
	// check if compressor is getting sent
	c.ctype = RegionClient
	mockConn.EXPECT().Write(gomock.Any()).Return(78, nil).Times(1).Do(func(buf []byte) {
		expected := []byte("HBas\x00P\x00\x00\x00J\n\x06\n\x04root\x12\rClientService\x1a+" +
			"org.apache.hadoop.hbase.codec.KeyValueCodec\"\x04mock")
		if !bytes.Equal(expected, buf) {
			t.Errorf("expected %q, got %q", expected, buf)
		}
	})
	err = c.sendHello()
	if err != nil {
		t.Errorf("Was expecting error, but got one: %#v", err)
	}
}

func TestFail(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn: mockConn,
		done: make(chan struct{}),
		rpcs: make(chan hrpc.Call),
		sent: make(map[uint32]hrpc.Call),
	}
	expectedErr := errors.New("oooups")

	// check that connection Close is called only once
	mockConn.EXPECT().Close().Times(1)

	// run multiple in parallel to make sure everything is called only once
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			c.fail(expectedErr)
			wg.Done()
		}()
	}
	wg.Wait()

	// check if done channel is closed to notify goroutines to stop
	// if close(c.done) is called more than once, it would panic
	select {
	case <-time.After(2 * time.Second):
		t.Errorf("done hasn't been closed")
	case _, more := <-c.done:
		if more {
			t.Error("expected done to be closed")
		}
	}

	// check that failing undialed client doesn't panic
	c = &client{
		done: make(chan struct{}),
		rpcs: make(chan hrpc.Call),
		sent: make(map[uint32]hrpc.Call),
	}
	c.fail(expectedErr)
}

type mc struct {
	*mock.MockConn

	closed uint32
}

func (c *mc) Write(b []byte) (n int, err error) {
	if v := atomic.LoadUint32(&c.closed); v != 0 {
		return 0, errors.New("closed connection")
	}
	return 0, nil
}

func (c *mc) Close() error {
	atomic.AddUint32(&c.closed, 1)
	return nil
}

func TestQueueRPCMultiWithClose(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()

	ncalls := 1000

	c := &client{
		conn:          &mc{MockConn: mockConn},
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  10,
		flushInterval: 30 * time.Millisecond,
	}

	var wgProcessRPCs sync.WaitGroup
	wgProcessRPCs.Add(1)
	go func() {
		c.processRPCs()
		wgProcessRPCs.Done()
	}()

	calls := make([]hrpc.Call, ncalls)
	for i := range calls {
		call, err := hrpc.NewGet(context.Background(), []byte("yolo"), []byte("swag"))
		if err != nil {
			t.Fatal(err)
		}
		call.SetRegion(reg0)
		calls[i] = call
	}

	for _, call := range calls {
		go c.QueueRPC(call)
	}

	c.Close()

	// check that all calls are not stuck and get an error
	for _, call := range calls {
		r := <-call.ResultChan()
		if r.Error == nil {
			t.Error("got no error")
		}
	}

	wgProcessRPCs.Wait()
}

type rpcMatcher struct {
	payload []byte
}

func (m rpcMatcher) Matches(x interface{}) bool {
	data, ok := x.([]byte)
	if !ok {
		return false
	}
	return bytes.HasSuffix(data, m.payload)
}

func (m rpcMatcher) String() string {
	return fmt.Sprintf("RPC payload is equal to %q", m.payload)
}

func newRPCMatcher(payload []byte) gomock.Matcher {
	return rpcMatcher{payload: payload}
}

func TestProcessRPCsWithFail(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().Close()

	ncalls := 100

	c := &client{
		conn: mockConn,
		rpcs: make(chan hrpc.Call),
		done: make(chan struct{}),
		sent: make(map[uint32]hrpc.Call),
		// never send anything
		rpcQueueSize:  ncalls + 1,
		flushInterval: 1000 * time.Hour,
	}

	var wgProcessRPCs sync.WaitGroup
	wgProcessRPCs.Add(1)
	go func() {
		c.processRPCs()
		wgProcessRPCs.Done()
	}()

	calls := make([]hrpc.Call, ncalls)
	for i := range calls {
		call, err := hrpc.NewGet(context.Background(), []byte("yolo"), []byte("swag"))
		if err != nil {
			t.Fatal(err)
		}
		call.SetRegion(reg0)
		calls[i] = call
	}

	for _, call := range calls {
		c.QueueRPC(call)
	}

	c.fail(errors.New("OOOPS"))

	// check that all calls are not stuck and get an error
	for _, call := range calls {
		r := <-call.ResultChan()
		if r.Error != ErrClientClosed {
			t.Errorf("got unexpected error %v, expected %v", r.Error, ErrClientClosed)
		}
	}

	wgProcessRPCs.Wait()
}

func mockRPCProto(row string) (proto.Message, []byte) {
	regionType := pb.RegionSpecifier_REGION_NAME
	r := &pb.RegionSpecifier{
		Type:  &regionType,
		Value: []byte("yolo"),
	}
	get := &pb.GetRequest{Region: r, Get: &pb.Get{Row: []byte(row)}}

	var b []byte
	var err error
	b = protowire.AppendVarint(b, uint64(proto.Size(get)))
	b, err = proto.MarshalOptions{}.MarshalAppend(b, get)
	if err != nil {
		panic(err)
	}
	return get, b
}

func TestQueueRPC(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	queueSize := 30
	flushInterval := 20 * time.Millisecond
	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	var wgProcessRPCs sync.WaitGroup
	wgProcessRPCs.Add(1)
	go func() {
		c.processRPCs() // Writer goroutine
		wgProcessRPCs.Done()
	}()

	// define rpcs behaviour
	var wgWrites sync.WaitGroup
	calls := make([]hrpc.Call, 100)
	ctx := context.Background()
	for i := range calls {
		wgWrites.Add(1)
		mockCall := mock.NewMockCall(ctrl)
		mockCall.EXPECT().Name().Return("Get").Times(1)
		p, payload := mockRPCProto(fmt.Sprintf("rpc_%d", i))
		mockCall.EXPECT().ToProto().Return(p).Times(1)
		mockCall.EXPECT().Context().Return(ctx).Times(1)
		mockCall.EXPECT().ResultChan().Return(make(chan hrpc.RPCResult, 1)).Times(1)
		calls[i] = mockCall

		// we expect that it eventually writes to connection
		mockConn.EXPECT().Write(newRPCMatcher(payload)).Times(1).Return(14+len(payload), nil).Do(
			func(buf []byte) { wgWrites.Done() })
	}

	// queue calls in parallel
	for _, call := range calls {
		go func(call hrpc.Call) {
			c.QueueRPC(call)
		}(call)
	}

	// wait till all writes complete
	wgWrites.Wait()

	// check sent map
	if len(c.sent) != len(calls) {
		t.Errorf("expected len(c.sent) be %d, got %d", len(calls), len(c.sent))
	}

	var wg sync.WaitGroup
	// now we fail the regionserver, and try to queue stuff
	mockConn.EXPECT().Close().Times(1)
	c.fail(errors.New("ooups"))
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := make(chan hrpc.RPCResult, 1)
			mockCall := mock.NewMockCall(ctrl)
			mockCall.EXPECT().Context().Return(ctx).Times(1)
			mockCall.EXPECT().ResultChan().Return(result).Times(1)
			c.QueueRPC(mockCall)
			r := <-result
			err, ok := r.Error.(ServerError)
			if !ok {
				t.Errorf("Expected ServerError error")
				return
			}
			if ErrClientClosed != err {
				t.Errorf("expected %v, got %v", ErrClientClosed, err)
			}
		}()
	}
	wg.Wait()
	wgProcessRPCs.Wait()
}

func TestServerErrorWrite(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	queueSize := 1
	flushInterval := 10 * time.Millisecond
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	// define rpcs behaviour
	mockCall := mock.NewMockCall(ctrl)
	p, payload := mockRPCProto("rpc")
	mockCall.EXPECT().ToProto().Return(p).Times(1)
	mockCall.EXPECT().Name().Return("Get").Times(1)
	mockCall.EXPECT().Context().Return(context.Background()).Times(1)
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	// we expect that it eventually writes to connection
	expErr := errors.New("Write failure")
	mockConn.EXPECT().Write(newRPCMatcher(payload)).Times(1).Return(0, expErr)
	mockConn.EXPECT().Close()

	c.QueueRPC(mockCall)
	// check that processRPCs exists
	c.processRPCs()
	r := <-result
	if ErrClientClosed != r.Error {
		t.Errorf("expected %v, got %v", ErrClientClosed, r.Error)
	}
	if len(c.sent) != 0 {
		t.Errorf("Expected all awaiting rpcs to be processed, %d left", len(c.sent))
	}
}

func TestServerErrorRead(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	queueSize := 1
	flushInterval := 10 * time.Millisecond
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	// define rpcs behavior
	mockCall := mock.NewMockCall(ctrl)
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	mockConn.EXPECT().Read([]byte{0, 0, 0, 0}).Return(0, errors.New("read failure"))
	mockConn.EXPECT().Close()

	// pretend we already unqueued and sent the rpc
	c.sent[1] = mockCall
	// now try receiving result, should call fail
	c.receiveRPCs()
	_, more := <-c.done
	if more {
		t.Error("expected done to be closed")
	}
	// finish reading from c.rpc to clean up the c.sent map
	c.processRPCs()
	if len(c.sent) != 0 {
		t.Errorf("Expected all awaiting rpcs to be processed, %d left", len(c.sent))
	}
	r := <-result
	if ErrClientClosed != r.Error {
		t.Errorf("expected %v, got %v", ErrClientClosed, r.Error)
	}
}

func TestServerErrorExceptionResponse(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().SetReadDeadline(gomock.Any()).Times(2)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  1,
		flushInterval: 1000 * time.Second,
	}

	rpc, err := hrpc.NewGetStr(context.Background(), "test1", "yolo")
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}

	c.registerRPC(rpc)
	if err := c.inFlightUp(); err != nil {
		t.Fatal(err)
	}

	var response []byte
	header := &pb.ResponseHeader{
		CallId: proto.Uint32(1),
		Exception: &pb.ExceptionResponse{
			ExceptionClassName: proto.String(
				"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException"),
			StackTrace: proto.String("ooops"),
		},
	}

	response = protowire.AppendVarint(response, uint64(proto.Size(header)))
	response, err = proto.MarshalOptions{}.MarshalAppend(response, header)
	if err != nil {
		t.Fatal(err)
	}

	mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).Times(1).Return(4, nil).
		Do(func(buf []byte) { binary.BigEndian.PutUint32(buf, uint32(len(response))) })

	mockConn.EXPECT().Read(readBufSizeMatcher{l: len(response)}).Times(1).
		Return(len(response), nil).Do(func(buf []byte) { copy(buf, response) })

	expErr := exceptionToError(
		"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException", "ooops")

	err = c.receive()
	if _, ok := err.(ServerError); !ok {
		if err.Error() != expErr.Error() {
			t.Fatalf("expected ServerError with message %q, got %T: %v", expErr, err, err)
		}
	}

	re := <-rpc.ResultChan()
	if re.Error != err {
		t.Errorf("expected error %v, got %v", err, re.Error)
	}
}

func TestExceptionToError(t *testing.T) {
	tcases := []struct {
		class string
		stack string
		out   error
	}{
		{
			class: "java.io.IOException",
			stack: "ooops",
			out:   errors.New("HBase Java exception java.io.IOException:\nooops"),
		},
		{
			class: "java.io.IOException",
			stack: "Cannot append; log is closed\nblahblah",
			out: NotServingRegionError{errors.New("HBase Java exception java.io.IOException:\n" +
				"Cannot append; log is closed\nblahblah")},
		},
		{
			class: "org.apache.hadoop.hbase.CallQueueTooBigException",
			stack: "blahblah",
			out: RetryableError{errors.New(
				"HBase Java exception org.apache.hadoop.hbase.CallQueueTooBigException:\n" +
					"blahblah")},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.class, func(t *testing.T) {
			err := exceptionToError(tcase.class, tcase.stack)
			if !reflect.DeepEqual(err, tcase.out) {
				t.Fatalf("expected error %q, got error %q", tcase.out, err)
			}
		})
	}
}

func TestReceiveDecodeProtobufError(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn: mockConn,
		done: make(chan struct{}),
		sent: make(map[uint32]hrpc.Call),
	}

	mockCall := mock.NewMockCall(ctrl)
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	mockCall.EXPECT().NewResponse().Return(&pb.MutateResponse{}).Times(1)
	mockCall.EXPECT().Context().Return(context.Background()).Times(1)

	c.sent[1] = mockCall
	c.inFlight = 1

	// Append mutate response with a chunk in the middle missing
	response := []byte{6, 8, 1, 26, 2, 8, 38, 34, 0, 0, 0, 22,
		0, 0, 0, 4, 0, 4, 121, 111, 108, 111, 2, 99, 102, 115, 119, 97, 103, 0, 0, 0, 0, 0, 0,
		0, 0, 4, 109, 101, 111, 119}
	mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).Times(1).Return(4, nil).
		Do(func(buf []byte) { binary.BigEndian.PutUint32(buf, uint32(len(response))) })
	mockConn.EXPECT().Read(readBufSizeMatcher{l: len(response)}).Times(1).
		Return(len(response), nil).Do(func(buf []byte) { copy(buf, response) })
	mockConn.EXPECT().SetReadDeadline(time.Time{}).Times(1)
	expErrorPefix := "region.RetryableError: failed to decode the response: proto:"

	err := c.receive()
	if err == nil || strings.HasPrefix(expErrorPefix, err.Error()) {
		t.Errorf("Expected error prefix %v, got %v", expErrorPefix, err)
	}

	res := <-result
	if err != res.Error {
		t.Errorf("Expected error %v, got %v", err, res.Error)
	}
}

type callWithCellBlocksError struct{ hrpc.Call }

func (cwcbe callWithCellBlocksError) DeserializeCellBlocks(proto.Message, []byte) (uint32, error) {
	return 0, errors.New("OOPS")
}

func TestReceiveDeserializeCellblocksError(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn: mockConn,
		done: make(chan struct{}),
		sent: make(map[uint32]hrpc.Call),
	}

	mockCall := mock.NewMockCall(ctrl)
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	mockCall.EXPECT().NewResponse().Return(&pb.MutateResponse{}).Times(1)
	mockCall.EXPECT().Context().Return(context.Background()).Times(1)

	c.sent[1] = callWithCellBlocksError{mockCall}
	c.inFlight = 1

	// Append mutate response
	response := []byte{6, 8, 1, 26, 2, 8, 38, 6, 10, 4, 16, 1, 32, 0, 0, 0, 0, 34, 0, 0, 0, 22,
		0, 0, 0, 4, 0, 4, 121, 111, 108, 111, 2, 99, 102, 115, 119, 97, 103, 0, 0, 0, 0, 0, 0,
		0, 0, 4, 109, 101, 111, 119}
	mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).Times(1).Return(4, nil).
		Do(func(buf []byte) { binary.BigEndian.PutUint32(buf, uint32(len(response))) })
	mockConn.EXPECT().Read(readBufSizeMatcher{l: len(response)}).Times(1).
		Return(len(response), nil).Do(func(buf []byte) { copy(buf, response) })
	mockConn.EXPECT().SetReadDeadline(time.Time{}).Times(1)
	expError := errors.New("region.RetryableError: failed to decode the response: OOPS")

	err := c.receive()
	if err == nil || err.Error() != expError.Error() {
		t.Errorf("Expected error %v, got %v", expError, err)
	}

	res := <-result
	if err != res.Error {
		t.Errorf("Expected error %v, got %v", err, res.Error)
	}
}

func TestUnexpectedSendError(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	queueSize := 1
	flushInterval := 10 * time.Millisecond
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	go c.processRPCs()
	// define rpcs behaviour
	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().ToProto().Return(nil).Times(1)
	mockCall.EXPECT().Context().Return(context.Background()).Times(1)
	result := make(chan hrpc.RPCResult, 1)
	mockCall.EXPECT().ResultChan().Return(result).Times(1)
	mockCall.EXPECT().Name().Return("Whatever").Times(1)

	c.QueueRPC(mockCall)
	r := <-result
	expectedErr := "failed to marshal request: proto: Marshal called with nil"
	if err := r.Error; err == nil || err.Error() != expectedErr {
		t.Errorf("expected %q, got %v", expectedErr, err)
	}
	if len(c.sent) != 0 {
		t.Errorf("Expected all awaiting rpcs to be processed, %d left", len(c.sent))
	}
	// stop the go routine
	mockConn.EXPECT().Close()
	c.Close()
}

func TestProcessRPCs(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		qsize      int
		interval   time.Duration
		ncalls     int
		minsent    int
		maxsent    int
		concurrent bool
	}{
		{qsize: 100000, interval: 30 * time.Millisecond, ncalls: 100,
			minsent: 1, maxsent: 1},
		{qsize: 2, interval: 1000 * time.Hour, ncalls: 100, minsent: 50, maxsent: 50},
		{qsize: 100, interval: 0 * time.Millisecond, ncalls: 1000,
			minsent: 1, maxsent: 1000, concurrent: true},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			mockConn := mock.NewMockConn(ctrl)
			mockConn.EXPECT().Close()

			c := &client{
				conn:          mockConn,
				rpcs:          make(chan hrpc.Call),
				done:          make(chan struct{}),
				sent:          make(map[uint32]hrpc.Call),
				rpcQueueSize:  tcase.qsize,
				flushInterval: tcase.interval,
			}

			var wgProcessRPCs sync.WaitGroup
			wgProcessRPCs.Add(1)
			go func() {
				c.processRPCs()
				wgProcessRPCs.Done()
			}()

			var sent int32
			var wgWrite sync.WaitGroup
			wgWrite.Add(tcase.minsent)
			mockConn.EXPECT().Write(gomock.Any()).MinTimes(tcase.minsent).
				MaxTimes(tcase.maxsent).Return(42, nil).Do(func(buf []byte) {
				if atomic.AddInt32(&sent, 1) <= int32(tcase.minsent) {
					wgWrite.Done() // test will timeout if didn't get at least minsent
				}
			})
			mockConn.EXPECT().SetReadDeadline(gomock.Any()).
				MinTimes(tcase.minsent).MaxTimes(tcase.maxsent)

			calls := make([]hrpc.Call, tcase.ncalls)
			for i := range calls {
				call, err := hrpc.NewGet(context.Background(), []byte("yolo"), []byte("swag"))
				if err != nil {
					t.Fatal(err)
				}
				call.SetRegion(reg0)
				calls[i] = call
			}

			if tcase.concurrent {
				var wg sync.WaitGroup
				for _, call := range calls {
					wg.Add(1)
					go func(call hrpc.Call) {
						c.QueueRPC(call)
						wg.Done()
					}(call)
				}
				wg.Wait()
			} else {
				for _, call := range calls {
					c.QueueRPC(call)
				}
			}

			wgWrite.Wait()

			c.Close()
			wgProcessRPCs.Wait()
			t.Log("num batches sent", sent)

			if f := int(c.inFlight); f < tcase.minsent || f > tcase.maxsent {
				t.Errorf("expected [%d:%d] in-flight rpcs, got %d",
					tcase.minsent, tcase.maxsent, c.inFlight)
			}
		})
	}
}

func TestRPCContext(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	queueSize := 10
	flushInterval := 1000 * time.Second
	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().Close()
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}

	mockConn.EXPECT().SetReadDeadline(gomock.Any()).Times(1)

	// queue rpc with background context
	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Name().Return("Get").Times(1)
	p, payload := mockRPCProto("yolo")
	mockCall.EXPECT().ToProto().Return(p).Times(1)
	mockCall.EXPECT().Context().Return(context.Background()).Times(1)
	mockCall.EXPECT().ResultChan().Return(make(chan hrpc.RPCResult, 1)).Times(1)
	mockConn.EXPECT().Write(newRPCMatcher(payload)).Times(1).Return(14+len(payload), nil)
	c.QueueRPC(mockCall)

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	callWithCancel := mock.NewMockCall(ctrl)
	callWithCancel.EXPECT().Context().Return(ctxCancel).Times(1)
	// this shouldn't block
	c.QueueRPC(callWithCancel)

	if int(c.inFlight) != 1 {
		t.Errorf("expected %d in-flight rpcs, got %d", 1, c.inFlight)
	}
	// clean up
	c.Close()
}

type readBufSizeMatcher struct {
	l int
}

func (r readBufSizeMatcher) Matches(x interface{}) bool {
	data, ok := x.([]byte)
	if !ok {
		return false
	}
	return len(data) == r.l
}

func (r readBufSizeMatcher) String() string {
	return fmt.Sprintf("buf size is equal to %q", r.l)
}

func TestSanity(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  1, // size one to skip sendBatch
		flushInterval: 1000 * time.Second,
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		c.processRPCs()
		wg.Done()
	}()

	// using Append as it returns cellblocks
	app, err := hrpc.NewAppStr(context.Background(), "test1", "yolo",
		map[string]map[string][]byte{"cf": map[string][]byte{"swag": []byte("meow")}})
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	app.SetRegion(
		NewInfo(0, nil, []byte("test1"), []byte("test1,,lololololololololololo"), nil, nil))

	mockConn.EXPECT().Write(gomock.Any()).Times(8).Return(0, nil)
	mockConn.EXPECT().SetReadDeadline(gomock.Any()).Times(1)

	c.QueueRPC(app)

	response := []byte{6, 8, 1, 26, 2, 8, 38, 6, 10, 4, 16, 1, 32, 0, 0, 0, 0, 34, 0, 0, 0, 22,
		0, 0, 0, 4, 0, 4, 121, 111, 108, 111, 2, 99, 102, 115, 119, 97, 103, 0, 0, 0, 0, 0, 0,
		0, 0, 4, 109, 101, 111, 119}
	mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).Times(1).Return(4, nil).
		Do(func(buf []byte) {
			binary.BigEndian.PutUint32(buf, uint32(len(response)))
		})
	mockConn.EXPECT().Read(readBufSizeMatcher{l: len(response)}).Times(1).
		Return(len(response), nil).Do(func(buf []byte) {
		copy(buf, response)

		// stall the next read
		mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).MaxTimes(1).
			Return(0, errors.New("closed")).Do(func(buf []byte) { <-c.done })
	})
	mockConn.EXPECT().SetReadDeadline(time.Time{}).Times(1)
	wg.Add(1)
	go func() {
		c.receiveRPCs()
		wg.Done()
	}()

	re := <-app.ResultChan()
	if re.Error != nil {
		t.Error(re.Error)
	}
	r, ok := re.Msg.(*pb.MutateResponse)
	if !ok {
		t.Fatalf("got unexpected type %T for response", r)
	}
	expResult := &pb.Result{
		AssociatedCellCount: proto.Int32(1),
		Stale:               proto.Bool(false),
		Cell: []*pb.Cell{
			&pb.Cell{
				Row:       []byte("yolo"),
				Family:    []byte("cf"),
				Qualifier: []byte("swag"),
				Value:     []byte("meow"),
				CellType:  pb.CellType_PUT.Enum(),
				Timestamp: proto.Uint64(0),
			},
		},
	}
	if !proto.Equal(expResult, r.Result) {
		t.Errorf("expected %v, got %v", expResult, r.Result)
	}
	if int(c.inFlight) != 0 {
		t.Errorf("expected %d in-flight rpcs, got %d", 0, c.inFlight)
	}

	mockConn.EXPECT().Close().Times(1)
	c.Close()
	wg.Wait()
}

func TestSanityCompressor(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn:          mockConn,
		rpcs:          make(chan hrpc.Call),
		done:          make(chan struct{}),
		sent:          make(map[uint32]hrpc.Call),
		rpcQueueSize:  1, // size one to skip sendBatch
		flushInterval: 1000 * time.Second,
		compressor:    &compressor{Codec: mockCodec{}},
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		c.processRPCs()
		wg.Done()
	}()

	// using Append as it returns cellblocks
	app, err := hrpc.NewAppStr(context.Background(), "test1", "yolo",
		map[string]map[string][]byte{"cf": map[string][]byte{"swag": []byte("meow")}})
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	app.SetRegion(
		NewInfo(0, nil, []byte("test1"), []byte("test1,,lololololololololololo"), nil, nil))

	compressedCellblocks := "\x00\x00\x00&" +
		"\x00\x00\x00\n\x00\x00\x00\"\x00\x00\x00\x16\x00\x00" +
		"\x00\x00\x00\n\x00\x04\x00\x04yolo\x02c" +
		"\x00\x00\x00\nfswag\u007f\xff\xff\xff\xff" +
		"\x00\x00\x00\b\xff\xff\xff\x04meow"

	mockConn.EXPECT().Write([]byte("\x00\x00\x00}\x10\b\x01\x1a\x06Mutate \x01*\x02\b:1\n!"+
		"\b\x01\x12\x1dtest1,,lololololololololololo\x12\f\n\x04yolo\x10\x000\x00@\x01")).Return(
		71, nil)
	mockConn.EXPECT().Write([]byte(compressedCellblocks)).Return(58, nil)
	mockConn.EXPECT().SetReadDeadline(gomock.Any())

	c.QueueRPC(app)

	header := &pb.ResponseHeader{
		CallId: proto.Uint32(1),
		CellBlockMeta: &pb.CellBlockMeta{
			Length: proto.Uint32(58),
		},
	}

	mutate := &pb.MutateResponse{
		Result: &pb.Result{
			AssociatedCellCount: proto.Int32(1),
			Stale:               proto.Bool(false),
		},
	}

	var response []byte
	response = protowire.AppendVarint(response, uint64(proto.Size(header)))
	response, err = proto.MarshalOptions{}.MarshalAppend(response, header)
	if err != nil {
		t.Fatal(err)
	}
	response = protowire.AppendVarint(response, uint64(proto.Size(mutate)))
	response, err = proto.MarshalOptions{}.MarshalAppend(response, mutate)
	if err != nil {
		t.Fatal(err)
	}

	response = append(response, []byte(compressedCellblocks)...)
	mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).Times(1).Return(4, nil).
		Do(func(buf []byte) {
			binary.BigEndian.PutUint32(buf, uint32(len(response)))
		})
	mockConn.EXPECT().Read(readBufSizeMatcher{l: len(response)}).Times(1).
		Return(len(response), nil).Do(func(buf []byte) {
		copy(buf, response)

		// stall the next read
		mockConn.EXPECT().Read(readBufSizeMatcher{l: 4}).MaxTimes(1).
			Return(0, errors.New("closed")).Do(func(buf []byte) { <-c.done })
	})
	mockConn.EXPECT().SetReadDeadline(time.Time{}).Times(1)
	wg.Add(1)
	go func() {
		c.receiveRPCs()
		wg.Done()
	}()

	re := <-app.ResultChan()
	if re.Error != nil {
		t.Error(re.Error)
	}
	r, ok := re.Msg.(*pb.MutateResponse)
	if !ok {
		t.Fatalf("got unexpected type %T for response", r)
	}
	expResult := &pb.Result{
		AssociatedCellCount: proto.Int32(1),
		Stale:               proto.Bool(false),
		Cell: []*pb.Cell{
			&pb.Cell{
				Row:       []byte("yolo"),
				Family:    []byte("cf"),
				Qualifier: []byte("swag"),
				Value:     []byte("meow"),
				CellType:  pb.CellType_PUT.Enum(),
				Timestamp: proto.Uint64(math.MaxInt64),
			},
		},
	}
	if !proto.Equal(expResult, r.Result) {
		t.Errorf("expected %v, got %v", expResult, r.Result)
	}
	if int(c.inFlight) != 0 {
		t.Errorf("expected %d in-flight rpcs, got %d", 0, c.inFlight)
	}

	mockConn.EXPECT().Close().Times(1)
	c.Close()
	wg.Wait()
}

func BenchmarkSendBatchMemory(b *testing.B) {
	ctrl := test.NewController(b)
	defer ctrl.Finish()
	mockConn := mock.NewMockConn(ctrl)
	c := &client{
		conn: mockConn,
		rpcs: make(chan hrpc.Call),
		done: make(chan struct{}),
		sent: make(map[uint32]hrpc.Call),
		// queue size is 1 so that all QueueRPC calls trigger sendBatch,
		// and buffer slice reset
		rpcQueueSize:  1,
		flushInterval: 1000 * time.Second,
	}

	var wgWrites sync.WaitGroup
	ctx := context.Background()
	mockCall := mock.NewMockCall(ctrl)
	mockCall.EXPECT().Name().Return("Get").AnyTimes()
	p, _ := mockRPCProto("rpc")
	mockCall.EXPECT().ToProto().Return(p).AnyTimes()
	mockCall.EXPECT().Context().Return(ctx).AnyTimes()
	mockConn.EXPECT().Write(gomock.Any()).AnyTimes().Return(0, nil).Do(func(buf []byte) {
		wgWrites.Done()
	})

	go c.processRPCs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 100; i++ {
			wgWrites.Add(1)
			c.QueueRPC(mockCall)
		}
		wgWrites.Wait()
	}
	// we don't care about cleaning up
}

func BenchmarkSetReadDeadline(b *testing.B) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			b.Error(err)
		}
		wg.Done()
		conn.Close()
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.SetReadDeadline(time.Now().Add(DefaultReadTimeout)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	conn.Close()
	l.Close()
	wg.Wait()
}

func TestBuffer(t *testing.T) {
	size := 42
	b := newBuffer(size)
	if cap(b) < size {
		t.Fatalf("Excpected cap >= %d, got %d", size, cap(b))
	}
	if len(b) != size {
		t.Fatalf("Excpected len %d, got %d", size, len(b))
	}
	freeBuffer(b)

	size = 40
	b = newBuffer(size)
	if cap(b) < size {
		t.Fatalf("Excpected cap >= %d, got %d", size, cap(b))
	}
	if len(b) != size {
		t.Fatalf("Excpected len %d, got %d", size, len(b))
	}
	freeBuffer(b)

	size = 45
	b = newBuffer(size)
	if cap(b) < size {
		t.Fatalf("Excpected cap >= %d, got %d", size, cap(b))
	}
	if len(b) != size {
		t.Fatalf("Excpected len %d, got %d", size, len(b))
	}
	freeBuffer(b)
}
