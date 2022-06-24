// Copyright 2018 The CFS Authors.
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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/raftstore/walreader/decode"
	"github.com/chubaofs/chubaofs/raftstore/walreader/sink"
)

func main() {

	var (
		walPath     string
		file        string
		decoderName string
		keyword     string
		start       uint64
		count       uint64
		filter      string
		showHelp    bool
		force       bool
		err         error

	)

	flag.StringVar(&walPath, "path", "", "path of RAFT WAL")
	flag.StringVar(&file, "file", "", "file name of RAFT WAL")
	flag.StringVar(&decoderName, "decoder", "meta",
		fmt.Sprintf("decoder name for WAL (%v)", strings.Join(decode.RegisteredDecoders(), ",")))
	flag.StringVar(&keyword, "keyword", "", "keyword of decoded entry")
	flag.Uint64Var(&start, "start", 0, "start index to read")
	flag.Uint64Var(&count, "count", 0, "read limit, 0 for unlimited")
	flag.StringVar(&filter, "filter", "all", "record type filter (all,normal,confchange)")
	flag.BoolVar(&showHelp, "help", false, "show help")
	flag.BoolVar(&force, "force", false, "force parse raft log, no need meta file")
	flag.Parse()

	if showHelp {
		flag.Usage()
		return
	}

	if len(decoderName) == 0 {
		fmt.Printf("No decoder specified.\n")
		flag.Usage()
		return
	}

	var recordFilter sink.RecordFilter
	switch strings.ToLower(strings.TrimSpace(filter)) {
	case "all":
		recordFilter = sink.RecordFilter_All
	case "normal":
		recordFilter = sink.RecordFilter_Normal
	case "confchange":
		recordFilter = sink.RecordFilter_ConfChange
	default:
		fmt.Printf("Illegal filter \"%v\"\n", filter)
		flag.Usage()
		return
	}

	var decoderConstructor decode.DecoderConstructor
	if decoderConstructor = decode.GetDecoder(decoderName); decoderConstructor == nil {
		fmt.Printf("Decoder %v not found.\n", decoderName)
		return
	}
	var decoder decode.LogCommandDecoder
	if decoder, err = decoderConstructor(); err != nil {
		fmt.Printf("Decoder %v init failed: %v\n", decoderName, err)
		return
	}

	var opt = sink.Option{
		File:    file,
		Start:   start,
		Count:   count,
		Filter:  recordFilter,
		Keyword: keyword,
	}

	fmt.Printf("WAL path : %v\n", walPath)
	fmt.Printf("File     : %v\n", file)
	fmt.Printf("Keyword  : %v\n", keyword)
	fmt.Printf("Decoder  : %v\n", decoderName)
	fmt.Printf("Start    : %v\n", start)
	fmt.Printf("Count    : %s\n", func() string {
		if count == 0 {
			return "unlimited"
		}
		return strconv.FormatUint(count, 10)
	}())
	fmt.Printf("Filter   : %v\n", recordFilter)

	var bufWriter = bufio.NewWriter(os.Stdout)

	var sinker = sink.NewLogEntrySinker(walPath, decoder, bufWriter, opt)
	if force {
		sinker.ForceParseRaftLog()
	} else {

		if err = sinker.Run(); err != nil {
			_ = bufWriter.Flush()
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
	}

	_ = bufWriter.Flush()
	os.Exit(0)
	return
}
