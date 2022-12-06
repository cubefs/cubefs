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

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	xredis "github.com/go-redis/redis/v8"
)

var (
	// ErrNotFound redis key is not found
	ErrNotFound = errors.New("key not found")
	// ErrEncodeFailed encode failed
	ErrEncodeFailed = errors.New("encode failed")
	// ErrDecodeFailed decode failed
	ErrDecodeFailed = errors.New("decode failed")
)

// CodeC encode and decode value get/set redis
type CodeC interface {
	Encode() ([]byte, error)
	Decode(data []byte) error
}

// ClusterConfig redis cluster config
// TODO: add more options like redis.ClusterOptions
type ClusterConfig struct {
	Addrs    []string `json:"addrs"`
	Username string   `json:"username"`
	Password string   `json:"password"`

	// time seconds
	DialTimeout  int `json:"dial_timeout"`
	ReadTimeout  int `json:"read_timeout"`
	WriteTimeout int `json:"write_timeout"`
}

// ClusterClient some as xredis.ClusterClient
// TODO: more redis commands to implement
type ClusterClient struct {
	*xredis.ClusterClient
}

// NewClusterClient new a redis cluster client
func NewClusterClient(config *ClusterConfig) *ClusterClient {
	return &ClusterClient{
		ClusterClient: xredis.NewClusterClient(&xredis.ClusterOptions{
			Addrs:        config.Addrs[:],
			Username:     config.Username,
			Password:     config.Password,
			DialTimeout:  time.Duration(config.DialTimeout) * time.Second,
			ReadTimeout:  time.Duration(config.ReadTimeout) * time.Second,
			WriteTimeout: time.Duration(config.WriteTimeout) * time.Second,
		}),
	}
}

// Get value of key in redis, return no such key if value is nil
func (cc *ClusterClient) Get(ctx context.Context, key string, value interface{}) error {
	var (
		valBytes []byte
		err      error
	)

	valBytes, err = cc.ClusterClient.Get(ctx, key).Bytes()
	if err == xredis.Nil {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	if val, ok := value.(CodeC); ok {
		err = val.Decode(valBytes)
	} else {
		err = json.Unmarshal(valBytes, value)
	}
	if err != nil {
		return ErrDecodeFailed
	}

	return nil
}

// Set value of key to redis
func (cc *ClusterClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	var (
		valBytes []byte
		err      error
	)

	if val, ok := value.(CodeC); ok {
		valBytes, err = val.Encode()
	} else {
		valBytes, err = json.Marshal(value)
	}
	if err != nil {
		return ErrEncodeFailed
	}

	err = cc.ClusterClient.Set(ctx, key, valBytes, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}
