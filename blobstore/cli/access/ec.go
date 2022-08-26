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

package access

import (
	"strings"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
)

func showECbuffer(c *grumble.Context) error {
	kb := 1 << 10
	mb := 1 << 20
	perPage := 7

	blobSize := c.Args.Int("blobsize")
	if blobSize > 64*mb {
		return fmt.Errorf("too large blobsize %d", blobSize)
	}

	var modes, all []codemode.CodeMode
	all = codemode.GetAllCodeModes()
	mode := c.Flags.Int("codemode")
	for _, m := range all {
		if m == codemode.CodeMode(mode) {
			modes = []codemode.CodeMode{m}
			break
		}
	}
	if len(modes) == 0 {
		modes = all
	}

	for len(modes) > 0 {
		length := len(modes)
		hasNextPage := length > perPage
		if length > perPage {
			length = perPage
		}

		showModes := modes[:length]
		showEcbufferSize(-1, common.Optimal, showModes)
		if blobSize > 0 {
			showEcbufferSize(blobSize, common.Loaded, showModes)
			if hasNextPage {
				fmt.Println()
			}
			modes = modes[length:]
			continue
		}

		alterColor := common.NewAlternateColor(3)
		for _, size := range []int{
			1,
			kb * 2,
			kb * 12,
			kb * 64,
			kb * 512,
			mb * 1,
			mb * 2,
			mb * 4,
			mb * 8,
			mb * 16,
		} {
			showEcbufferSize(size, alterColor.Next(), showModes)
		}
		if hasNextPage {
			fmt.Println()
		}
		modes = modes[length:]
	}

	return nil
}

func showEcbufferSize(size int, colorFmt *color.Color, modes []codemode.CodeMode) {
	if size == -1 {
		colorFmt.Printf("|%s|", center("blobsize"))
		for _, mode := range modes {
			colorFmt.Printf("%s|", center(mode.String()))
		}
		fmt.Println()
		return
	}

	colorFmt.Printf("|%s|", center(size2Str(size)))
	for _, mode := range modes {
		sizes, _ := ec.GetBufferSizes(size, mode.Tactic())
		colorFmt.Printf("%s|", center(size2Str(sizes.ECSize)))
	}
	fmt.Println()

	colorFmt.Printf("|%s|", center("shard size"))
	for _, mode := range modes {
		sizes, _ := ec.GetBufferSizes(size, mode.Tactic())
		colorFmt.Printf("%s|", center(size2Str(sizes.ShardSize)))
	}
	fmt.Println()
}

func center(s string) string {
	all := 19
	l := (all - len(s)) / 2
	if l < 0 {
		l = 0
	}
	r := all - len(s) - l
	if r < 0 {
		r = 0
	}
	return fmt.Sprintf("%s%s%s", strings.Repeat(" ", l), s, strings.Repeat(" ", r))
}

func size2Str(size int) string {
	return fmt.Sprintf("%d(%s)", size, humanize.IBytes(uint64(size)))
}
