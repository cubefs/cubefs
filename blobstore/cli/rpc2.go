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
	"reflect"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

var types = map[string]func() rpc2.Codec{
	"shardnode.GetBlobArgs": func() rpc2.Codec { return new(shardnode.GetBlobArgs) },
	"shardnode.GetBlobRet":  func() rpc2.Codec { return new(shardnode.GetBlobRet) },

	"nil":              func() rpc2.Codec { return nil },
	"rpc2.NoParameter": func() rpc2.Codec { return rpc2.NoParameter },
}

func helpType(t any) string {
	if t == nil {
		return "nil"
	}
	typ := reflect.TypeOf(t)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		panic(typ.Name())
	}
	return typ.PkgPath() + "." + typ.Name()
}

func helps() (s string) {
	for n, ft := range types {
		s += "\n" + n + " -> " + helpType(ft())
	}
	return
}

func cmdRpc2Request(c *grumble.Context) error {
	cli := config.Rpc2Client
	addr, path, request := c.Args.String("addr"), c.Args.String("path"), c.Args.String("request")
	paraType, rstType := c.Flags.String("parameter"), c.Flags.String("result")

	paraf, exist := types[paraType]
	if !exist {
		return fmt.Errorf("not found parameter(%s), types:%s", paraType, helps())
	}
	para := paraf()
	if para != nil && para != rpc2.NoParameter {
		if err := common.Unmarshal([]byte(request), para); err != nil {
			return err
		}
	}
	if s, ok := para.(interface{ GoString() string }); ok {
		fmt.Println("parameter: " + s.GoString())
	} else if s, ok := para.(interface{ String() string }); ok {
		fmt.Println("parameter: " + s.String())
	} else {
		fmt.Println("parameter: " + common.Readable(para))
	}

	rstf, exist := types[rstType]
	if !exist {
		return fmt.Errorf("not found result(%s), types:%s", rstType, helps())
	}
	rst := rstf()
	if err := cli.Request(common.CmdContext(), addr, path, para, rst); err != nil {
		return err
	}
	fmt.Println("result: ", common.Readable(rst))
	return nil
}

func registerRpc2(app *grumble.App) {
	rpc2Command := &grumble.Command{
		Name:     "rpc2",
		Help:     "simple client of rpc2",
		LongHelp: "rpc2 simple client struct request and response, types:\n" + helps(),
		Args: func(a *grumble.Args) {
			a.String("addr", "request address")
			a.String("path", "request path")
			a.String("request", "request parameter json value")
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("parameter", "", "request parameter name")
			f.StringL("result", "", "response result name")
		},
		Run: cmdRpc2Request,
	}
	app.AddCommand(rpc2Command)
}
