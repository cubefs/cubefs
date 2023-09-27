// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type RedisClient interface {
	redis.Cmdable
	Close() error
}

type RedisConfig struct {
	Address               string `json:"address"`
	Password              string `json:"password"`
	PoolSize              int    `json:"pool_size"`
	DialTimeoutMs         int    `json:"dial_timeout_ms"`
	ReadTimeoutMs         int    `json:"read_timeout_ms"`
	WriteTimeoutMs        int    `json:"write_timeout_ms"`
	IdleTimeoutSec        int    `json:"idle_timeout_sec"`
	IdleCheckFrequencySec int    `json:"idle_check_frequency_sec"`
	EnableClusterMode     bool   `json:"enable_cluster_mode"`
}

// FixConfig validates and fixes the configuration.
func (c *RedisConfig) FixConfig() error {
	if c.Address == "" || len(strings.Split(c.Address, ",")) <= 0 {
		return errors.New("redis: no address found")
	}

	if c.DialTimeoutMs <= 0 {
		c.DialTimeoutMs = 3000
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 5
	}
	if c.ReadTimeoutMs <= 0 {
		c.ReadTimeoutMs = 3000
	}
	if c.WriteTimeoutMs <= 0 {
		c.WriteTimeoutMs = 3000
	}
	if c.IdleTimeoutSec <= 0 {
		c.IdleTimeoutSec = 300
	}
	if c.IdleCheckFrequencySec <= 0 {
		c.IdleCheckFrequencySec = 60
	}

	return nil
}

// BuildClient creates a RedisClient.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *RedisConfig) BuildClient() (RedisClient, error) {
	var client RedisClient
	addrs := strings.Split(c.Address, ",")
	if len(addrs) == 1 && !c.EnableClusterMode {
		client = redis.NewClient(&redis.Options{
			Addr:               addrs[0],
			Password:           c.Password,
			PoolSize:           c.PoolSize,
			DB:                 0,
			DialTimeout:        time.Duration(c.DialTimeoutMs) * time.Millisecond,
			ReadTimeout:        time.Duration(c.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout:       time.Duration(c.WriteTimeoutMs) * time.Millisecond,
			IdleTimeout:        time.Duration(c.IdleTimeoutSec) * time.Second,
			IdleCheckFrequency: time.Duration(c.IdleCheckFrequencySec) * time.Second,
		})
	} else {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:              addrs,
			Password:           c.Password,
			PoolSize:           c.PoolSize,
			MaxRedirects:       2,
			DialTimeout:        time.Duration(c.DialTimeoutMs) * time.Millisecond,
			ReadTimeout:        time.Duration(c.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout:       time.Duration(c.WriteTimeoutMs) * time.Millisecond,
			IdleTimeout:        time.Duration(c.IdleTimeoutSec) * time.Second,
			IdleCheckFrequency: time.Duration(c.IdleCheckFrequencySec) * time.Second,
		})
	}

	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return client, nil
}

type RedisNotifierConfig struct {
	Enable bool   `json:"enable"`
	Key    string `json:"key"`

	RedisConfig
}

func (c *RedisNotifierConfig) FixConfig() error {
	if err := c.RedisConfig.FixConfig(); err != nil {
		return err
	}
	if c.Key == "" {
		c.Key = "cubefs:objectnode:event"
	}

	return nil
}

type RedisNotifier struct {
	id     NotifierID
	client RedisClient

	RedisNotifierConfig
}

func NewRedisNotifier(id string, conf RedisNotifierConfig) (*RedisNotifier, error) {
	if err := conf.FixConfig(); err != nil {
		return nil, err
	}

	client, err := conf.BuildClient()
	if err != nil {
		return nil, err
	}

	return &RedisNotifier{
		id:                  NotifierID{ID: id, Name: "redis"},
		client:              client,
		RedisNotifierConfig: conf,
	}, nil
}

func (r *RedisNotifier) ID() NotifierID {
	return r.id
}

func (r *RedisNotifier) Name() string {
	return r.id.String()
}

func (r *RedisNotifier) Send(data []byte) error {
	return r.client.RPush(r.Key, data).Err()
}

func (r *RedisNotifier) Close() error {
	var err error
	if r.client != nil {
		err = r.client.Close()
	}

	return err
}
