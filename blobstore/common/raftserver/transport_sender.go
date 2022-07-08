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

package raftserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	inputChannelSize    = 128
	maxMsgSize          = 64 * 1024 * 1024
	maxIdleConns        = 10
	maxIdleConnsPerHost = 5
	rwBufferSize        = 16 * 1024
	idleConnTimeout     = 30 * time.Second
	requestTimeout      = 10 * time.Second
)

type transportSender struct {
	nodeId  uint64
	msgUrl  string
	snapUrl string
	inputc  chan raftMsgs
	stopc   chan struct{}
	client  *http.Client
	once    sync.Once
}

func newTransportSender(nodeId uint64, host string) *transportSender {
	tr := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		WriteBufferSize:     rwBufferSize,
		ReadBufferSize:      rwBufferSize,
		IdleConnTimeout:     idleConnTimeout,
		DisableCompression:  true,
	}
	sender := &transportSender{
		nodeId:  nodeId,
		msgUrl:  fmt.Sprintf("http://%s%s", host, raftMsgUrl),
		snapUrl: fmt.Sprintf("http://%s%s", host, snapshotUrl),
		inputc:  make(chan raftMsgs, inputChannelSize),
		client:  &http.Client{Transport: tr},
		stopc:   make(chan struct{}),
	}

	go sender.loopSend()
	return sender
}

func (sender *transportSender) Send(msgs []pb.Message) {
	select {
	case sender.inputc <- raftMsgs(msgs):
	case <-sender.stopc:
	default:
	}
}

func (sender *transportSender) Stop() {
	sender.once.Do(func() {
		close(sender.stopc)
	})
}

func (sender *transportSender) loopSend() {
	buffer := &bytes.Buffer{}
	var errCnt uint64

	for {
		select {
		case msgs := <-sender.inputc:
			buffer.Reset()
			var size int
			for i := 0; i < len(msgs); i++ {
				size += msgs[i].Size()
			}
			for i := 0; i < inputChannelSize && size < maxMsgSize; i++ {
				var done bool
				select {
				case ms := <-sender.inputc:
					for i := 0; i < len(ms); i++ {
						size += ms[i].Size()
					}
					msgs = append(msgs, ms...)
				default:
					done = true
				}
				if done {
					break
				}
			}
			err := msgs.Encode(buffer)
			if err != nil {
				continue
			}
			req, err := http.NewRequest(http.MethodPut, sender.msgUrl, buffer)
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			req = req.WithContext(ctx)
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := sender.client.Do(req)
			cancel()
			if err != nil {
				errCnt++
				if errCnt == 1 {
					log.Errorf("sent msg to %s error: %v, msg count: %d", sender.msgUrl, err, len(msgs))
				}
				continue
			}
			resp.Body.Close()
			prev := errCnt
			errCnt = 0
			if prev > 0 {
				log.Infof("sent msg to %s success, peer has been recovered", sender.msgUrl)
			}
		case <-sender.stopc:
			return
		}
	}
}

func (sender *transportSender) SendSnapshot(snap *snapshot) error {
	rd, wr := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, sender.snapUrl, rd)
	if err != nil {
		return fmt.Errorf("New snapshot request error %v", err)
	}
	metaData, err := snap.meta.Marshal()
	if err != nil {
		return fmt.Errorf("marshal snapshot meta error %v", err)
	}
	go func() {
		var err error
		defer wr.CloseWithError(err)
		write := func(data []byte) error {
			b := make([]byte, 4)
			crc := crc32.NewIEEE()
			mw := io.MultiWriter(wr, crc)
			binary.BigEndian.PutUint32(b, uint32(len(data)))
			if _, err := wr.Write(b); err != nil {
				return err
			}
			if _, err := mw.Write(data); err != nil {
				return err
			}
			binary.BigEndian.PutUint32(b, crc.Sum32())
			if _, err := wr.Write(b); err != nil {
				return err
			}
			return nil
		}
		if err = write(metaData); err != nil {
			return
		}
		for {
			data, err := snap.Read()
			if err != nil {
				break
			}
			if err = write(data); err != nil {
				break
			}
		}
	}()

	log.Infof("send snapshot(%s) to node(%d) %s", snap.Name(), sender.nodeId, sender.snapUrl)
	resp, err := sender.client.Do(req)
	if err != nil {
		rd.CloseWithError(err)
		return fmt.Errorf("send snapshot error %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("send snapshot return unexpect code(%d)", resp.StatusCode)
	}
	log.Infof("send snapshot(%s) to node(%d) success", snap.Name(), sender.nodeId)
	return nil
}
