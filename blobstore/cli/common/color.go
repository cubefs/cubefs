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

	"github.com/cubefs/cubefs/blobstore/util"
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
func ColorizeFloat[T util.Float](ratio T) *color.Color {
	return Colorize(int(ratio * 100))
}

func percentIfFree(free bool, percent int) int {
	if free && percent == 0 {
		return -1
	}
	return percent
}

// ColorizeInteger colorize by any integer type
// When used < 0 (signed types only), it indicates free space calculation
func ColorizeInteger[T util.Integer](used, total T) *color.Color {
	return Colorize(percentIfFree(used < 0, int(used*100/total)))
}

// ColorizeIntegerFree colorize free space by any integer type
func ColorizeIntegerFree[T util.Integer](free, total T) *color.Color {
	return Colorize(percentIfFree(true, -int(free*100/total)))
}
