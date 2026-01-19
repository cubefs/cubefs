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
	"math/rand"
	"testing"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
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
			fmt.Print(alterColor.Next().Sprint(line + " "))
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
		c = common.ColorizeFloat(-float32(f))
		c.Printf("free(%.3f) unknow ", -f)
		fmt.Println()
	}
	fmt.Println()

	common.ColorizeInteger(0, 100).Printf("Int    --> used(%d/%d) white\n", 0, 100)
	common.ColorizeInteger(-1, 1000).Printf("Int    --> free(%d/%d) dead", -1, 10000)
	fmt.Println()

	common.ColorizeInteger(80, 100).Printf("Int    --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeInteger(-80, 100).Printf("Int    --> free(%d/%d) green\n", 80, 100)
	common.ColorizeInteger(int32(80), 100).Printf("Int32  --> used(%d/%d) yellow\n", 80, 100)
	common.ColorizeInteger(-80, int32(100)).Printf("Int32  --> free(%d/%d) green\n", 80, 100)
	common.ColorizeIntegerFree(uint64(80), 100).Printf("Uint64 --> free(%d/%d) green\n", 80, 100)
}
