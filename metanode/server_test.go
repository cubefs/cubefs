// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metanode

import (
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

// fakeMetadataManager implements MetadataManager for server handlePacket tests.
type fakeMetadataManager struct {
	handleErr error
}

func (f *fakeMetadataManager) Start() error { return nil }
func (f *fakeMetadataManager) Stop()        {}

func (f *fakeMetadataManager) HandleMetadataOperation(net.Conn, *Packet, string) error {
	return f.handleErr
}

func (f *fakeMetadataManager) GetPartition(uint64) (MetaPartition, error) {
	return nil, errors.New("fake: no partition")
}

func (f *fakeMetadataManager) GetLeaderPartitions() map[uint64]MetaPartition { return nil }

func (f *fakeMetadataManager) GetAllVolumes() *util.Set { return util.NewSet() }

func (f *fakeMetadataManager) checkVolVerList() error { return nil }

func (f *fakeMetadataManager) ReloadPartition(uint64) error { return nil }

func (f *fakeMetadataManager) UpdateQosLimit() {}

func (f *fakeMetadataManager) SetRocksdbDiskThreshold(float64) {}

var _ MetadataManager = (*fakeMetadataManager)(nil)

func TestMetaNode_buildServerAddr(t *testing.T) {
	t.Parallel()
	m := &MetaNode{listen: "9020", localAddr: "127.0.0.1", bindIp: true}
	require.Equal(t, "127.0.0.1:9020", m.buildServerAddr())
	m.bindIp = false
	require.Equal(t, ":9020", m.buildServerAddr())
}

func TestMetaNode_handleConnectionError(t *testing.T) {
	t.Parallel()
	m := &MetaNode{}
	m.handleConnectionError(nil, "ctx")
	m.handleConnectionError(errors.New("EOF"), "ctx")
	m.handleConnectionError(errors.New("broken pipe"), "ctx")
}

func TestMetaNode_configureTCPConn(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		m := &MetaNode{}
		m.configureTCPConn(c)
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	wg.Wait()

	m := &MetaNode{}
	m.configureTCPConn(&net.UnixConn{}) // not *TCPConn; should not panic
}

func TestMetaNode_handlePacketError(t *testing.T) {
	t.Parallel()
	m := &MetaNode{}
	p := &Packet{}

	m.handlePacketError(nil, p)

	warnCases := []struct {
		name string
		err  error
		code uint8
	}{
		{name: "over_quota", err: errors.New("volume over quota"), code: 0},
		{name: "inode_out_of_range", err: errors.New("inode ID out of range"), code: 0},
		{name: "unknown_meta_partition", err: errors.New("unknown meta partition 42"), code: 0},
		{name: "op_not_exist_result", err: errors.New("not found"), code: proto.OpNotExistErr},
		{name: "rate_limited", err: errors.New("request rate limited"), code: 0},
	}
	for _, tc := range warnCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pkt := &Packet{}
			pkt.ResultCode = tc.code
			m.handlePacketError(tc.err, pkt)
		})
	}

	t.Run("generic_logs_error", func(t *testing.T) {
		t.Parallel()
		pkt := &Packet{}
		m.handlePacketError(errors.New("internal failure"), pkt)
	})
}

func TestMetaNode_handlePacket(t *testing.T) {
	t.Parallel()
	m := &MetaNode{metadataManager: &fakeMetadataManager{}}
	err := m.handlePacket(nil, &Packet{}, "127.0.0.1:1")
	require.NoError(t, err)

	m.metadataManager = &fakeMetadataManager{handleErr: errors.New("handler boom")}
	err = m.handlePacket(nil, &Packet{}, "127.0.0.1:2")
	require.EqualError(t, err, "handler boom")
}

func TestMetaNode_startGenericServer_listenFails(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	addr := ln.Addr().String()
	m := &MetaNode{}
	stopC := make(chan uint8)
	err = m.startGenericServer(serverConfig{
		addr:    addr,
		stopC:   stopC,
		handler: func(net.Conn, chan uint8) {},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to listen")
}

func TestMetaNode_stopServer_idempotent(t *testing.T) {
	t.Parallel()
	m := &MetaNode{}
	m.stopServer()

	m.httpStopC = make(chan uint8)
	m.stopServer()
	m.stopServer()
}

func TestMetaNode_stopSmuxServer_nilStopC(t *testing.T) {
	t.Parallel()
	m := &MetaNode{}
	m.smuxStopC = nil
	m.stopSmuxServer()
}
