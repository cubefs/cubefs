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

package cli

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
)

func cmdTime(c *grumble.Context) error {
	unix := c.Args.String("unix")
	format := c.Args.String("format")

	if unix == "" {
		t := time.Now()
		fmt.Printf("timestamp = %s (seconds = %d nanosecs = %d) \n\t--> format: %s (%s)\n\n",
			color.RedString("%d", t.UnixNano()), t.Unix(), t.Nanosecond(),
			color.GreenString("%s", t.Format(time.RFC3339Nano)), humanize.Time(t))
		return nil
	}

	if format == "" {
		format = time.RFC3339Nano
	}

	unix += strings.Repeat("0", 19)
	sec, _ := strconv.ParseInt(unix[:10], 10, 64)
	nsec, _ := strconv.ParseInt(unix[10:19], 10, 64)

	t := time.Unix(sec, nsec)
	fmt.Printf("timestamp = %s (seconds = %d nanosecs = %d) \n\t--> format: %s (%s)\n\n",
		color.RedString("%s", c.Args.String("unix")), sec, nsec,
		color.GreenString("%s", t.Format(format)), humanize.Time(t))
	return nil
}

func cmdToken(c *grumble.Context) error {
	tokenStr := c.Args.String("token")
	token := uptoken.DecodeToken(tokenStr)
	err := fmt.Errorf("invalid token: %s", tokenStr)

	data := token.Data[:]
	offset := 8
	next := func() (uint64, bool) {
		val, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return 0, false
		}
		offset += n
		return val, true
	}

	var (
		minBid, count, expiredTime uint64
		ok                         bool
	)
	if minBid, ok = next(); !ok {
		return err
	}
	if count, ok = next(); !ok {
		return err
	}
	if expiredTime, ok = next(); !ok {
		return err
	}

	fmt.Println("Checksum: ", strings.ToUpper(hex.EncodeToString(data[0:8])))
	fmt.Println("MinBid  : ", minBid)
	fmt.Println("Count   : ", count)

	t := time.Unix(int64(expiredTime), 9)
	if time.Since(t) < 0 {
		fmt.Printf("Time    : %d (%s) (%s)", expiredTime,
			common.Normal.Sprint(t.Format(time.RFC3339)), humanize.Time(t))
	} else {
		fmt.Printf("Time    : %d (%s) (%s)", expiredTime,
			common.Danger.Sprint(t.Format(time.RFC3339)), humanize.Time(t))
	}
	fmt.Println()

	return nil
}

func registerUtil(app *grumble.App) {
	utilCommand := &grumble.Command{
		Name:     "util",
		Help:     "util commands",
		LongHelp: "util commands, parse everything",
	}
	app.AddCommand(utilCommand)

	utilCommand.AddCommand(&grumble.Command{
		Name:     "time",
		Help:     "time format [unix] [format]",
		LongHelp: "time format, show now if no argument",
		Run:      cmdTime,
		Args: func(a *grumble.Args) {
			a.String("unix", "unix timestamp", grumble.Default(""))
			a.String("format", "format for timestamp", grumble.Default(""))
		},
	})
	utilCommand.AddCommand(&grumble.Command{
		Name: "vuid",
		Help: "parse vuid <vuid>",
		Args: func(a *grumble.Args) {
			args.VuidRegister(a)
		},
		Run: func(c *grumble.Context) error {
			fmt.Println("Parse VUID: ", cfmt.VuidCF(args.Vuid(c.Args)))
			return nil
		},
	})
	utilCommand.AddCommand(&grumble.Command{
		Name: "token",
		Help: "parse token <token>",
		Run:  cmdToken,
		Args: func(a *grumble.Args) {
			a.String("token", "token of putat")
		},
	})

	utilCommand.AddCommand(&grumble.Command{
		Name:     "location",
		Help:     "parse location <[json|hex|base64]>",
		LongHelp: "parse location, or decode from string",
		Args: func(a *grumble.Args) {
			a.String("jsonORstr", "location json or location string")
		},
		Run: func(c *grumble.Context) error {
			jsonORstr := c.Args.String("jsonORstr")
			loc, err := cfmt.ParseLocation(jsonORstr)
			if err == nil {
				fmt.Println(cfmt.LocationJoin(&loc, ""))
				return nil
			}

			src, err := hex.DecodeString(jsonORstr)
			if err != nil {
				return fmt.Errorf("invalid (%s) %s", jsonORstr, err.Error())
			}
			loc, n, err := proto.DecodeLocation(src)
			if err != nil {
				fmt.Printf("has read bytes %d / %d\n", n, len(src))
				fmt.Println(cfmt.LocationJoin(&loc, ""))
				return fmt.Errorf("invalid (%s) %s", jsonORstr, err.Error())
			}

			fmt.Println(cfmt.LocationJoin(&loc, ""))
			return nil
		},
	})
}
