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

package common_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/cli/common"
)

func TestCmdCommonColorAlternate(t *testing.T) {
	lines := []string{
		"first-line",
		"second-line",
		"3th-line",
		"4th-line",
		"5th-line",
		"6th-line",
		"7th-line",
	}
	for _, n := range []int{-1, 0, 1, 2, 3, 4, 5, 6, 10} {
		alterColor := common.NewAlternateColor(n)
		fmt.Printf("%3d : ", n)
		for _, line := range lines {
			alterColor.Next().Print(line + " ")
		}
		fmt.Println()
	}
}

func TestCmdCommonColorRatio(t *testing.T) {
	cases := []struct {
		rand   int
		offset int
		s      string
	}{
		{1000, -1100, "White [Optimal]"},
		{20, 0, "white [Optimal]"},
		{20, 20, "green [Normal]"},
		{20, 40, "blue [Loaded]"},
		{25, 60, "yellow [Warn]"},
		{12, 85, "red [Danger]"},
		{3, 97, "highlight [Dead]"},
		{1000, 100, "highlight [Dead]"},
	}

	rand.Seed(time.Now().Unix())
	for _, cs := range cases {
		for i := 0; i < 10; i++ {
			percent := rand.Intn(cs.rand) + cs.offset
			c := common.Colorize(percent)
			c.Printf("used(%d) %s ", percent, cs.s)
			if cs.rand < 1000 {
				c = common.Colorize(percent - 100)
				c.Printf("| free(%d) %s", percent-100, cs.s)
			}
			fmt.Println()
		}
		fmt.Println()
	}

	for i := 0; i < 10; i++ {
		f := rand.Float64()
		c := common.ColorizeFloat(f)
		c.Printf("used(%.3f) unknow ", f)
		c = common.ColorizeFloat(-f)
		c.Printf("free(%.3f) unknow ", -f)
		fmt.Println()
	}
	fmt.Println()

	common.ColorizeInt(0, 100).Printf("ColorizeInt --> used(%d/%d) white\n", 0, 100)
	common.ColorizeInt(-1, 1000).Printf("ColorizeInt --> free(%d/%d) dead", -1, 10000)
	fmt.Println()

	common.ColorizeInt(80, 100).Printf("ColorizeInt --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeInt(-80, 100).Printf("ColorizeInt --> free(%d/%d) green\n", 80, 100)
	common.ColorizeInt32(80, 100).Printf("ColorizeInt32 --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeInt32(-80, 100).Printf("ColorizeInt32 --> free(%d/%d) green\n", 80, 100)
	common.ColorizeInt64(80, 100).Printf("ColorizeInt64 --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeInt64(-80, 100).Printf("ColorizeInt64 --> free(%d/%d) green\n", 80, 100)
	common.ColorizeUint32(80, 100).Printf("ColorizeUint32 --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeUint32Free(80, 100).Printf("ColorizeUint32Free --> free(%d/%d) green\n", 80, 100)
	common.ColorizeUint64(80, 100).Printf("ColorizeUint64 --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeUint64Free(80, 100).Printf("ColorizeUint64Free --> free(%d/%d) green\n", 80, 100)
}
