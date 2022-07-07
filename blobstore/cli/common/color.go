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

package common

import (
	"github.com/fatih/color"
)

// colorize defined
var (
	Optimal = color.New(color.FgHiWhite)
	Normal  = color.New(color.FgHiGreen)
	Loaded  = color.New(color.FgHiBlue)
	Warn    = color.New(color.FgHiYellow)
	Danger  = color.New(color.FgHiRed)
	Dead    = color.New(color.FgBlack, color.BgCyan)

	alternateColor = []*color.Color{
		color.New(color.FgHiWhite),
		color.New(color.FgWhite, color.Faint),
		color.New(color.FgHiBlue),
		color.New(color.FgBlue, color.Faint),
		color.New(color.FgHiGreen),
		color.New(color.FgGreen, color.Faint),
	}
	maxAlternateColor = len(alternateColor)
)

// AlternateColor alternate color formatter
type AlternateColor struct {
	index  int
	n      int
	colors []*color.Color
}

// NewAlternateColor returns an alternate color formatter
func NewAlternateColor(n int) *AlternateColor {
	if n <= 0 {
		n = 1
	}
	if n > maxAlternateColor {
		n = maxAlternateColor
	}
	return &AlternateColor{
		index:  0,
		n:      n,
		colors: alternateColor[:n],
	}
}

// Next returns next color
func (a *AlternateColor) Next() *color.Color {
	c := a.colors[a.index]
	a.index = (a.index + 1) % a.n
	return c
}

// Colorize by int [-100, 100] percent
// Danger --> Optimal --> Danger
func Colorize(percent int) *color.Color {
	if percent < -100 {
		percent = -100
	}
	if percent > 100 {
		percent = 100
	}
	if percent < 0 {
		percent = 100 + percent
	}

	switch {
	case percent < 20:
		return Optimal
	case percent < 40:
		return Normal
	case percent < 60:
		return Loaded
	case percent < 85:
		return Warn
	case percent < 97:
		return Danger
	default:
		return Dead
	}
}

// ColorizeFloat color by float64 [-1.0, 1.0] ratio
func ColorizeFloat(ratio float64) *color.Color {
	return Colorize(int(ratio * 100))
}

func percentIfFree(free bool, percent int) int {
	if free && percent == 0 {
		return -1
	}
	return percent
}

// ColorizeInt by int
func ColorizeInt(used, total int) *color.Color {
	return Colorize(percentIfFree(used < 0, used*100/total))
}

// ColorizeInt32 by int32
func ColorizeInt32(used, total int32) *color.Color {
	return Colorize(percentIfFree(used < 0, int(used*100/total)))
}

// ColorizeInt64 by int64
func ColorizeInt64(used, total int64) *color.Color {
	return Colorize(percentIfFree(used < 0, int(used*100/total)))
}

// ColorizeUint32 by uint32
func ColorizeUint32(used, total uint32) *color.Color {
	return Colorize(int(used * 100 / total))
}

// ColorizeUint64 by uint64
func ColorizeUint64(used, total uint64) *color.Color {
	return Colorize(int(used * 100 / total))
}

// ColorizeUint32Free free by uint32
func ColorizeUint32Free(free, total uint32) *color.Color {
	return Colorize(percentIfFree(true, -int(free*100/total)))
}

// ColorizeUint64Free free by uint64
func ColorizeUint64Free(free, total uint64) *color.Color {
	return Colorize(percentIfFree(true, -int(free*100/total)))
}
