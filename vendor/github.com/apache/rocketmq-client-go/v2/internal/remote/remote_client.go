/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package remote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type ClientRequestFunc func(*RemotingCommand, net.Addr) *RemotingCommand

type TcpOption struct {
	KeepAliveDuration time.Duration
	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
}

//go:generate mockgen -source remote_client.go -destination mock_remote_client.go -self_package github.com/apache/rocketmq-client-go/v2/internal/remote  --package remote RemotingClient
type RemotingClient interface {
	RegisterRequestFunc(code int16, f ClientRequestFunc)
	RegisterInterceptor(interceptors ...primitive.Interceptor)
	InvokeSync(ctx context.Context, addr string, request *RemotingCommand) (*RemotingCommand, error)
	InvokeAsync(ctx context.Context, addr string, request *RemotingCommand, callback func(*ResponseFuture)) error
	InvokeOneWay(ctx context.Context, addr string, request *RemotingCommand) error
	ShutDown()
}

var _ RemotingClient = &remotingClient{}

type remotingClient struct {
	responseTable    sync.Map
	connectionTable  sync.Map
	config           *RemotingClientConfig
	processors       map[int16]ClientRequestFunc
	connectionLocker sync.Mutex
	interceptor      primitive.Interceptor
}

type RemotingClientConfig struct {
	TcpOption
}

var DefaultRemotingClientConfig = RemotingClientConfig{defaultTcpOption}

var defaultTcpOption = TcpOption{
	KeepAliveDuration: 0, // default 15s in golang
	ConnectionTimeout: time.Second * 15,
	ReadTimeout:       time.Second * 120,
	WriteTimeout:      time.Second * 120,
}

func NewRemotingClient(config *RemotingClientConfig) *remotingClient {
	if config == nil {
		config = &DefaultRemotingClientConfig
	}

	return &remotingClient{
		processors: make(map[int16]ClientRequestFunc),
		config:     config,
	}
}

func (c *remotingClient) RegisterRequestFunc(code int16, f ClientRequestFunc) {
	c.processors[code] = f
}

// TODO: merge sync and async model. sync should run on async model by blocking on chan
func (c *remotingClient) InvokeSync(ctx context.Context, addr string, request *RemotingCommand) (*RemotingCommand, error) {
	conn, err := c.connect(ctx, addr)
	if err != nil {
		return nil, err
	}

	resp := NewResponseFuture(ctx, request.Opaque, nil)

	c.responseTable.Store(resp.Opaque, resp)
	defer c.responseTable.Delete(request.Opaque)

	err = c.sendRequest(conn, request)
	if err != nil {
		return nil, err
	}

	return resp.waitResponse()
}

// InvokeAsync send request without blocking, just return immediately.
func (c *remotingClient) InvokeAsync(ctx context.Context, addr string, request *RemotingCommand, callback func(*ResponseFuture)) error {
	conn, err := c.connect(ctx, addr)
	if err != nil {
		return err
	}

	resp := NewResponseFuture(ctx, request.Opaque, callback)
	c.responseTable.Store(resp.Opaque, resp)

	err = c.sendRequest(conn, request)
	if err != nil {
		c.responseTable.Delete(request.Opaque)
		return err
	}

	go primitive.WithRecover(func() {
		c.receiveAsync(resp)
		c.responseTable.Delete(request.Opaque)
	})

	return nil
}

func (c *remotingClient) receiveAsync(f *ResponseFuture) {
	_, err := f.waitResponse()
	if err != nil {
		f.executeInvokeCallback()
	}
}

func (c *remotingClient) InvokeOneWay(ctx context.Context, addr string, request *RemotingCommand) error {
	conn, err := c.connect(ctx, addr)
	if err != nil {
		return err
	}
	return c.sendRequest(conn, request)
}

func (c *remotingClient) connect(ctx context.Context, addr string) (*tcpConnWrapper, error) {
	//it needs additional locker.
	c.connectionLocker.Lock()
	defer c.connectionLocker.Unlock()
	conn, ok := c.connectionTable.Load(addr)
	if ok {
		return conn.(*tcpConnWrapper), nil
	}
	tcpConn, err := initConn(ctx, addr, c.config)
	if err != nil {
		return nil, err
	}
	c.connectionTable.Store(addr, tcpConn)
	go primitive.WithRecover(func() {
		c.receiveResponse(tcpConn)
	})
	return tcpConn, nil
}

func (c *remotingClient) receiveResponse(r *tcpConnWrapper) {
	var err error
	header := primitive.GetHeader()
	defer primitive.BackHeader(header)
	for {
		if err != nil {
			// conn has been closed actively
			if r.isClosed(err) {
				return
			}
			if err != io.EOF {
				rlog.Error("conn error, close connection", map[string]interface{}{
					rlog.LogKeyUnderlayError: err,
				})
			}
			c.closeConnection(r)
			r.destroy()
			break
		}

		err = r.Conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		if err != nil {
			continue
		}

		_, err = io.ReadFull(r, *header)
		if err != nil {
			continue
		}

		var length int32
		err = binary.Read(bytes.NewReader(*header), binary.BigEndian, &length)
		if err != nil {
			continue
		}

		buf := make([]byte, length)

		_, err = io.ReadFull(r, buf)
		if err != nil {
			continue
		}

		cmd, err := decode(buf)
		if err != nil {
			rlog.Error("decode RemotingCommand error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			continue
		}
		c.processCMD(cmd, r)
	}
}

func (c *remotingClient) processCMD(cmd *RemotingCommand, r *tcpConnWrapper) {
	if cmd.isResponseType() {
		resp, exist := c.responseTable.Load(cmd.Opaque)
		if exist {
			c.responseTable.Delete(cmd.Opaque)
			responseFuture := resp.(*ResponseFuture)
			go primitive.WithRecover(func() {
				responseFuture.ResponseCommand = cmd
				responseFuture.executeInvokeCallback()
				if responseFuture.Done != nil {
					close(responseFuture.Done)
				}
			})
		}
	} else {
		f := c.processors[cmd.Code]
		if f != nil {
			// single goroutine will be deadlock
			// TODO: optimize with goroutine pool, https://github.com/apache/rocketmq-client-go/v2/issues/307
			go primitive.WithRecover(func() {
				res := f(cmd, r.RemoteAddr())
				if res != nil {
					res.Opaque = cmd.Opaque
					res.Flag |= 1 << 0
					err := c.sendRequest(r, res)
					if err != nil {
						rlog.Warning("send response to broker error", map[string]interface{}{
							rlog.LogKeyUnderlayError: err,
							"responseCode":           res.Code,
						})
					}
				}
			})
		} else {
			rlog.Warning("receive broker's requests, but no func to handle", map[string]interface{}{
				"responseCode": cmd.Code,
			})
		}
	}
}

func (c *remotingClient) createScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)

	// max batch size: 32, max message size: 4Mb
	scanner.Buffer(make([]byte, 1024*1024), 128*1024*1024)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		defer func() {
			if err := recover(); err != nil {
				rlog.Error("scanner split panic", map[string]interface{}{
					"panic": err,
				})
			}
		}()
		if !atEOF {
			if len(data) >= 4 {
				var length int32
				err := binary.Read(bytes.NewReader(data[0:4]), binary.BigEndian, &length)
				if err != nil {
					rlog.Error("split data error", map[string]interface{}{
						rlog.LogKeyUnderlayError: err,
					})
					return 0, nil, err
				}

				if int(length)+4 <= len(data) {
					return int(length) + 4, data[4 : length+4], nil
				}
			}
		}
		return 0, nil, nil
	})
	return scanner
}

func (c *remotingClient) sendRequest(conn *tcpConnWrapper, request *RemotingCommand) error {
	var err error
	if c.interceptor != nil {
		err = c.interceptor(context.Background(), request, nil, func(ctx context.Context, req, reply interface{}) error {
			return c.doRequest(conn, request)
		})
	} else {
		err = c.doRequest(conn, request)
	}
	return err
}

func (c *remotingClient) doRequest(conn *tcpConnWrapper, request *RemotingCommand) error {
	conn.Lock()
	defer conn.Unlock()

	err := conn.Conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	if err != nil {
		rlog.Error("conn error, close connection", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})

		c.closeConnection(conn)
		conn.destroy()
		return err
	}

	err = request.WriteTo(conn)
	if err != nil {
		rlog.Error("conn error, close connection", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})

		c.closeConnection(conn)
		conn.destroy()
		return err
	}

	return nil
}

func (c *remotingClient) closeConnection(toCloseConn *tcpConnWrapper) {
	c.connectionTable.Range(func(key, value interface{}) bool {
		if value == toCloseConn {
			c.connectionTable.Delete(key)
			return false
		} else {
			return true
		}
	})
}

func (c *remotingClient) ShutDown() {
	c.responseTable.Range(func(key, value interface{}) bool {
		c.responseTable.Delete(key)
		return true
	})
	c.connectionTable.Range(func(key, value interface{}) bool {
		conn := value.(*tcpConnWrapper)
		err := conn.destroy()
		if err != nil {
			rlog.Warning("close remoting conn error", map[string]interface{}{
				"remote":                 conn.RemoteAddr(),
				rlog.LogKeyUnderlayError: err,
			})
		}
		return true
	})
}

func (c *remotingClient) RegisterInterceptor(interceptors ...primitive.Interceptor) {
	c.interceptor = primitive.ChainInterceptors(interceptors...)
}
