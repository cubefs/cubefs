// Copyright 2024 The CubeFS Authors.
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

package cli

import (
	"io"
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

type (
	sCodec struct{ val string }
	fCodec struct{ val []byte }
)

var (
	_ rpc2.Codec = (*sCodec)(nil)
	_ rpc2.Codec = (*fCodec)(nil)
)

func (c *sCodec) Size() int                       { return len(c.val) }
func (c *sCodec) Marshal() ([]byte, error)        { return []byte(c.val), nil }
func (c *sCodec) MarshalTo(b []byte) (int, error) { return copy(b, []byte(c.val)), nil }
func (c *sCodec) Unmarshal(b []byte) error        { c.val = string(b); return nil }
func (c *sCodec) Readable() bool                  { return true }

func (c *fCodec) Size() int                       { return len(c.val) }
func (c *fCodec) Marshal() ([]byte, error)        { return c.val, nil }
func (c *fCodec) MarshalTo(b []byte) (int, error) { return copy(b, c.val), nil }
func (c *fCodec) Unmarshal(b []byte) error        { c.val = append(c.val, b...); return nil }

func cmdRpc2Request(c *grumble.Context) error {
	cli := config.Rpc2Client
	addr, path := c.Args.String("addr"), c.Args.String("path")
	rr := c.Flags.Bool("readable")
	paraVal, rstVal := c.Flags.String("parameter"), c.Flags.String("result")

	var para, rst rpc2.Codec
	if rr {
		para, rst = &sCodec{val: paraVal}, &sCodec{val: rstVal}
	} else {
		f, err := os.Open(paraVal)
		if err != nil {
			return err
		}
		val, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		f.Close()
		para, rst = &fCodec{val: val}, &fCodec{}
	}

	if err := cli.Request(common.CmdContext(), addr, path, para, rst); err != nil {
		return err
	}

	if rr {
		fmt.Println("result:", rst.(*sCodec).val)
	} else {
		f, err := os.OpenFile(rstVal, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		f.Write(rst.(*fCodec).val)
		f.Close()
	}
	return nil
}

func registerRpc2(app *grumble.App) {
	rpc2Command := &grumble.Command{
		Name:     "rpc2",
		Help:     "simple client of rpc2",
		LongHelp: "rpc2 simple client struct request and response",
		Args: func(a *grumble.Args) {
			a.String("addr", "request address")
			a.String("path", "request path")
		},
		Flags: func(f *grumble.Flags) {
			f.BoolL("readable", false, "para and args is readable")
			f.StringL("parameter", "", "request parameter")
			f.StringL("result", "", "response result")
		},
		Run: cmdRpc2Request,
	}
	app.AddCommand(rpc2Command)
}
