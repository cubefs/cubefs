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

package mongoutil

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Config for mongo client
type Config struct {
	URI          string              `json:"uri"`
	TimeoutMs    int64               `json:"timeout_ms"`
	WriteConcern *WriteConcernConfig `json:"write_concern"`
	ReadConcern  string              `json:"read_concern"`
}

// for detail：https://docs.mongodb.com/manual/reference/write-concern/。
type WriteConcernConfig struct {
	Majority  bool  `json:"majority"`
	TimeoutMs int64 `json:"timeout_ms"`
}

var DefaultWriteConfig = WriteConcernConfig{
	Majority:  true,
	TimeoutMs: 3000,
}

const (
	// ReadConcernLocal mean https://docs.mongodb.com/manual/reference/read-concern-local/#readconcern.%22local%22
	ReadConcernLocal = "local"
	// ReadConcernAvailable mean https://docs.mongodb.com/manual/reference/read-concern-available/#readconcern.%22available%22
	ReadConcernAvailable = "available"
	// ReadConcernMajority mean https://docs.mongodb.com/manual/reference/read-concern-majority/#readconcern.%22majority%22
	ReadConcernMajority = "majority"
	// ReadConcernLinearizable mean https://docs.mongodb.com/manual/reference/read-concern-linearizable/#readconcern.%22linearizable%22
	ReadConcernLinearizable = "linearizable"
	// ReadConcernSnapshot mean https://docs.mongodb.com/manual/reference/read-concern-snapshot/#readconcern.%22snapshot%22
	ReadConcernSnapshot = "snapshot"
)

func checkValidWriteConcern(s string) error {
	switch s {
	case ReadConcernLocal, ReadConcernAvailable, ReadConcernMajority, ReadConcernLinearizable, ReadConcernSnapshot:
		return nil
	default:
		return errors.New("invalid write concern")
	}
}

func GetClient(conf Config) (*mongo.Client, error) {
	opt := options.Client().ApplyURI(conf.URI)
	if conf.TimeoutMs > 0 {
		timeoutDur := time.Duration(conf.TimeoutMs) * time.Millisecond
		opt.SetConnectTimeout(timeoutDur)
		opt.SetServerSelectionTimeout(timeoutDur)
		opt.SetSocketTimeout(timeoutDur)
	}
	if wcConf := conf.WriteConcern; wcConf != nil {
		var wcOpts []writeconcern.Option
		if wcConf.Majority {
			wcOpts = append(wcOpts, writeconcern.WMajority())
		}
		if wcConf.TimeoutMs > 0 {
			wcOpts = append(wcOpts, writeconcern.WTimeout(time.Duration(wcConf.TimeoutMs)*time.Millisecond))
		}
		wc := writeconcern.New(wcOpts...)
		opt.SetWriteConcern(wc)
	}
	if conf.ReadConcern != "" {
		if err := checkValidWriteConcern(conf.ReadConcern); err != nil {
			return nil, err
		}
		opt.SetReadConcern(readconcern.New(readconcern.Level(conf.ReadConcern)))
	}

	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, err
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	return client, nil
}

// IsDupError : err is mongo E11000?。
func IsDupError(err error) bool {
	return strings.Contains(err.Error(), "E11000")
}
