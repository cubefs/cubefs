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

package example

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var (
	errExceed    = rpc.NewError(http.StatusNotAcceptable, "MaxFiles", errors.New("exceed max files"))
	errBadRequst = rpc.NewError(http.StatusBadRequest, "BadRequest", errors.New("bad request"))
	errForbidden = rpc.NewError(http.StatusForbidden, "Forbidden", errors.New("forbidden"))
	errNotFound  = rpc.NewError(http.StatusNotFound, "NotFound", errors.New("not found"))
)

// Mode file mode
type Mode uint8

// file mode
const (
	_ Mode = iota
	ModeRW
	ModeW
	ModeR
	ModeNone
)

// FileApp app handlers
type FileApp interface {
	Upload(*rpc.Context)
	Update(*rpc.Context)
	Delete(*rpc.Context)
	Download(*rpc.Context)
	Stream(*rpc.Context)
	Exist(*rpc.Context)
	Stat(*rpc.Context)
	List(*rpc.Context)
	OptionalArgs(*rpc.Context)
}

// AppConfig app configure
type AppConfig struct {
	MaxFiles int `json:"max_files"`

	SimpleConfig SimpleConfig `json:"simple_config"`
	LBConfig     LBConfig     `json:"lb_config"`
}

type fileStat struct {
	Name  string
	Size  int
	Mode  Mode
	Desc  []byte
	Ctime time.Time
	Mtime time.Time
	Meta  map[string]string
}

type app struct {
	mu    sync.RWMutex
	files map[string]*fileStat

	fileCli Client
	metaCli Client
	config  AppConfig
}

// NewApp new app
func NewApp(cfg AppConfig) FileApp {
	return &app{
		files:   make(map[string]*fileStat, cfg.MaxFiles),
		fileCli: NewFileClient(&cfg.SimpleConfig),
		metaCli: NewMetaClient(&cfg.LBConfig, nil),
		config:  cfg,
	}
}

// ArgsUpload args upload
// you should register the args to rpc, cos field name in tag
type ArgsUpload struct {
	Name    string      `flag:"name"`        // required
	Size    int         `flag:"size"`        // required
	Mode    Mode        `flag:"mode"`        // required
	Desc    []byte      `flag:"desc,base64"` // required, base64 urlencode string
	Nothing interface{} `flag:"-"`           // ignored
}

func (a *app) Upload(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsUpload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("receive upload request, args: %#v", args)

	if args.Name == "" || args.Mode > ModeNone {
		c.RespondError(errBadRequst)
		return
	}

	a.mu.RLock()
	if len(a.files) >= a.config.MaxFiles {
		a.mu.RUnlock()
		c.RespondError(errExceed)
		return
	}
	a.mu.RUnlock()

	// append module cost, u should add track log before c.Respond*
	startBody := time.Now().Add(-100 * time.Millisecond)
	if err := a.fileCli.Write(c.Request.Context(), args.Size, c.Request.Body); err != nil {
		span.AppendTrackLog("body", startBody, err)
		c.RespondError(rpc.NewError(http.StatusGone, "ReadBody", err))
		return
	}
	span.AppendTrackLog("body", startBody, nil)

	var metaData []byte = []byte("meta")
	// construct meta data, write by rpc to server
	if err := a.metaCli.Write(c.Request.Context(), len(metaData), bytes.NewReader(metaData)); err != nil {
		c.RespondError(rpc.NewError(http.StatusGone, "ReadMeta", err))
		return
	}

	a.mu.Lock()
	now := time.Now()
	a.files[args.Name] = &fileStat{
		Name:  args.Name,
		Size:  args.Size,
		Mode:  args.Mode,
		Desc:  args.Desc[:],
		Ctime: now,
		Mtime: now,
		Meta:  make(map[string]string),
	}
	a.mu.Unlock()

	c.Respond()
}

// ArgsUpdate args update
// args in body, you can implement rpc.Unmarshaler to unmarshal
type ArgsUpdate struct {
	Name string
	Desc []byte
	Meta map[string]string
}

func (a *app) Update(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsUpdate)
	if err := c.ArgsBody(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("update: %+v", args)

	if args.Name == "" {
		c.RespondError(errBadRequst)
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	file, ok := a.files[args.Name]
	if !ok {
		c.RespondStatus(http.StatusNotFound)
		return
	}

	for k, v := range args.Meta {
		file.Meta[k] = v
	}
	file.Desc = args.Desc[:]
	file.Mtime = time.Now()

	c.Respond()
}

// ArgsDelete args delete
// args in query string, the key in getter is lowercase of field name
type ArgsDelete struct {
	Name string // key == name
	Mode Mode   // key == mode
}

func (a *app) Delete(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsDelete)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("delete: %+v", args)

	if args.Name == "" {
		c.RespondError(errBadRequst)
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	file, ok := a.files[args.Name]
	if !ok {
		c.RespondStatus(http.StatusNoContent)
		return
	}

	if args.Mode > file.Mode {
		c.RespondError(errForbidden)
		return
	}

	delete(a.files, args.Name)
	c.Respond()
}

// ArgsDownload args download
// you can define mulits tag on fields, but need to specify
// tag name's order in RegisterArgsParser
type ArgsDownload struct {
	Name   string `form:"name"`
	Mode   Mode   `form:"mode"`
	Offset int    `formx:"offset,omitempty" flag:"xxx"`
	Read   int    `formx:"read,omitempty" flag:"vvv"`
}

func (a *app) Download(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsDownload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("receive download request, args: %#v", args)

	a.mu.RLock()
	file, ok := a.files[args.Name]
	a.mu.RUnlock()

	if args.Name == "" || args.Offset < 0 || args.Read < 0 ||
		args.Offset+args.Read > file.Size {
		c.RespondError(errBadRequst)
		return
	}

	if !ok {
		c.RespondError(errNotFound)
		return
	}

	if args.Mode > file.Mode {
		c.RespondError(errForbidden)
		return
	}

	if args.Read == 0 {
		c.Respond()
		return
	}

	span.AppendTrackLog("body", time.Now().Add(-1*time.Second), nil)

	reader, err := a.fileCli.Read(c.Request.Context(), args.Read)
	if err != nil {
		span.Error(err)
		c.RespondError(err)
		return
	}
	defer reader.Close()

	// Note: rpc server do not handler response write-error
	if args.Offset+args.Read < file.Size {
		c.RespondWithReader(http.StatusPartialContent, args.Read, rpc.MIMEStream,
			reader, map[string]string{
				rpc.HeaderContentRange: fmt.Sprintf("bytes %d-%d/%d",
					args.Offset, args.Offset+args.Read-1, file.Size),
			})
	} else {
		c.RespondWithReader(http.StatusOK, args.Read, rpc.MIMEStream, reader, nil)
	}
}

func (a *app) Stream(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	c.RespondStatus(http.StatusMultiStatus)
	if disconnected := c.Stream(func(w io.Writer) bool {
		reader, err := a.fileCli.Read(c.Request.Context(), 1<<30)
		if err != nil {
			span.Warn(err)
			return false
		}
		defer reader.Close()

		_, err = io.CopyN(w, reader, 1<<30)
		if err != nil {
			span.Warn(err)
			return false
		}
		return true
	}); disconnected {
		span.Warn("client gone")
	}
}

// ArgsExist args exist or not
type ArgsExist struct {
	Name string
}

// Parse implements rpc.Parser, parse args by yourself
func (args *ArgsExist) Parse(getter rpc.ValueGetter) error {
	name := getter("queryname")
	if name == "" {
		name = getter("name")
	}
	if name == "" {
		return errors.New("empty name")
	}
	args.Name = name
	return nil
}

var _ rpc.Parser = &ArgsExist{}

func (a *app) Exist(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	args := new(ArgsExist)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("exist: %+v", args)

	a.mu.RLock()
	file, ok := a.files[args.Name]
	a.mu.RUnlock()
	if !ok {
		c.RespondError(errNotFound)
		return
	}

	c.Writer.Header().Set("x-file-name", file.Name)
	c.Writer.Header().Set("x-file-size", strconv.Itoa(file.Size))
	c.Writer.Header().Set("x-file-mode", strconv.Itoa(int(file.Mode)))
	c.Writer.Header().Set("x-file-desc", base64.URLEncoding.EncodeToString(file.Desc))
	c.Writer.Header().Set("x-file-ctime", file.Ctime.Format(time.RFC1123))
	c.Writer.Header().Set("x-file-mtime", file.Mtime.Format(time.RFC1123))

	for k, v := range file.Meta {
		c.Writer.Header().Set("x-file-meta-"+k, v)
	}

	c.Respond()
}

// RespStat args stat, use json tag
type RespStat struct {
	Name  string `json:"name"`
	Size  int    `json:"size"`
	Mode  Mode
	Desc  []byte `json:"desc"`
	Ctime int64
	Mtime int64
	Meta  map[string]string
}

var _ rpc.Marshaler = &RespStat{}

// Marshal implements rpc.Marshaler, define you own marshaler
func (stat *RespStat) Marshal() ([]byte, string, error) {
	b, err := json.Marshal(stat)
	return b, "application/x-json", err
}

func (a *app) Stat(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	args := new(ArgsExist)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("stat: %+v", args)

	a.mu.RLock()
	defer a.mu.RUnlock()
	file, ok := a.files[args.Name]
	if !ok {
		c.RespondError(errNotFound)
		return
	}

	resp := RespStat{
		Name:  file.Name,
		Size:  file.Size,
		Mode:  file.Mode,
		Desc:  make([]byte, len(file.Desc)),
		Ctime: file.Ctime.Unix(),
		Mtime: file.Mtime.Unix(),
		Meta:  make(map[string]string, len(file.Meta)),
	}
	copy(resp.Desc, file.Desc)
	for k, v := range file.Meta {
		resp.Meta[k] = v
	}

	c.RespondJSON(resp)
}

func (a *app) List(c *rpc.Context) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	files := make([]RespStat, 0, len(a.files))
	for _, file := range a.files {
		resp := RespStat{
			Name:  file.Name,
			Size:  file.Size,
			Mode:  file.Mode,
			Desc:  make([]byte, len(file.Desc)),
			Ctime: file.Ctime.Unix(),
			Mtime: file.Mtime.Unix(),
			Meta:  make(map[string]string, len(file.Meta)),
		}
		copy(resp.Desc, file.Desc)
		for k, v := range file.Meta {
			resp.Meta[k] = v
		}

		files = append(files, resp)
	}

	c.RespondStatusData(http.StatusOK, files)
}

// ArgsURIOptional argument in uri with omitempty
type ArgsURIOptional struct {
	Require string `json:"require"`
	Option  string `json:"option,omitempty"`
}

func (a *app) OptionalArgs(c *rpc.Context) {
	args := new(ArgsURIOptional)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(args)
}
