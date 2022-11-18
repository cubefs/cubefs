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

package qos

import (
	"context"
	"io"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	prio "github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

type IOQos struct {
	LevelMgr LevelGetter     // Identify: a level qos controller
	StatMgr  flow.StatGetter // Identify: a io flow
}

type Qos interface {
	ReaderAt(context.Context, bnapi.IOType, io.ReaderAt) io.ReaderAt
	WriterAt(context.Context, bnapi.IOType, io.WriterAt) io.WriterAt
	Writer(context.Context, bnapi.IOType, io.Writer) io.Writer
	Reader(context.Context, bnapi.IOType, io.Reader) io.Reader
	GetLevelMgr() LevelGetter
}

func (qos *IOQos) GetLevelMgr() LevelGetter {
	return qos.LevelMgr
}

func (qos *IOQos) getiostat(iot bnapi.IOType) (ios iostat.StatMgrAPI) {
	if qos.StatMgr != nil {
		ios = qos.StatMgr.GetStatMgr(iot)
	}
	return ios
}

func (qos *IOQos) ReaderAt(ctx context.Context, ioType bnapi.IOType, reader io.ReaderAt) (r io.ReaderAt) {
	r = reader

	if ios := qos.getiostat(ioType); ios != nil {
		r = ios.ReaderAt(reader)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		r = level.ReaderAt(ctx, r)
	}

	return r
}

func (qos *IOQos) WriterAt(ctx context.Context, ioType bnapi.IOType, writer io.WriterAt) (w io.WriterAt) {
	w = writer

	if ios := qos.getiostat(ioType); ios != nil {
		w = ios.WriterAt(writer)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		w = level.WriterAt(ctx, w)
	}

	return w
}

func (qos *IOQos) Writer(ctx context.Context, ioType bnapi.IOType, writer io.Writer) (w io.Writer) {
	w = writer

	if ios := qos.getiostat(ioType); ios != nil {
		w = ios.Writer(writer)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		w = level.Writer(ctx, w)
	}

	return w
}

func (qos *IOQos) Reader(ctx context.Context, ioType bnapi.IOType, reader io.Reader) (r io.Reader) {
	r = reader

	if ios := qos.getiostat(ioType); ios != nil {
		r = ios.Reader(reader)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		r = level.Reader(ctx, r)
	}

	return r
}

func NewQosManager(conf Config) (Qos, error) {
	// disk multi-level flow control
	levelMgr, err := NewLevelQosMgr(conf, conf.DiskViewer)
	if err != nil {
		return nil, err
	}

	qos := &IOQos{
		LevelMgr: levelMgr,
		StatMgr:  conf.StatGetter,
	}

	return qos, nil
}
