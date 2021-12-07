// Copyright 2020 The Chubao Authors.
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

package cmd

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"strconv"
	"time"
)

func newUSToStrCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "ustodate",
		Short: "parse a microsecond  to date",
		Run: func(cmd *cobra.Command, args []string) {
			str, err := USToDate(args[0])
			if err != nil {
				log.LogErrorf("failed to parse time: %v, err: %v", args[0], err.Error())
				return
			}
			fmt.Printf("==> %v\n", str)
		},
	}
	return c
}

func newDateToUSCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "datetous",
		Short: "parse a date to microsecond.",
		Run: func(cmd *cobra.Command, args []string) {
			us, err := DateToUS(args[0])
			if err != nil {
				log.LogErrorf("failed to parse time: %v, err: %v", args[0], err.Error())
				return
			}
			fmt.Printf("==> %v\n", us)
		},
	}
	return c
}

func USToDate(timeStr string) (str string, err error) {
	var val int
	val, err = strconv.Atoi(timeStr)
	if err != nil {
		return
	}

	var ts int64 = int64(val)
	str = time.Unix(ts/1000/1000, ts%1000000*1000).Format(dentryNameTimeFormat)
	return
}

func DateToUS(str string) (ts int64, err error) {
	loc, _ := time.LoadLocation("Local")
	var tm time.Time
	tm, err = time.ParseInLocation(dentryNameTimeFormat, str, loc)
	if err != nil {
		log.LogErrorf("parseDeletedName: str: %v, err: %v", str, err.Error())
		return
	}
	ts = tm.UnixNano() / 1000
	return
}
