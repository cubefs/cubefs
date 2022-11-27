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

package auditlog

import (
	"sort"
	"strings"
)

const (
	// upper letter[A-Z]
	UpperLetter = 0x01
	// lower letter[a-z]
	LowerLetter = 0x02
	// digital[0-9]
	Digital = 0x04
	// underline[_]
	Underline = 0x08
	// xdigit[0-9a-fA-F]
	HexDigital = 0x10
	// [\r\n]
	Newline = 0x20
	// [+]
	AddLetter = 0x40
	// [-]
	SubLetter = 0x80
	// [*]
	Asterisk = 0x100
	// [/]
	SlantLetter = 0x200
	// [<]
	LtLetter = 0x400
	// [>]
	GtLetter = 0x800
	// [=]
	EqLetter = 0x1000
	// [\\]
	RSlantLetter = 0x2000
	// [.]
	DotLetter = 0x4000
	// [:], colon
	ColonLetter = 0x8000
	// [%]
	PercentLetter = 0x10000
	// [&]
	AndLetter = 0x20000
	// [|]
	OrLetter = 0x40000
	// [ ]
	SpaceLetter = 0x80000
	// [r]
	RLetter = 0x100000
	// [t]
	TLetter = 0x200000
	// [n]
	NLetter = 0x400000
	// [w]
	WLetter = 0x800000
	// [,]
	CommaLetter = 0x1000000
	// [;]
	SemiColonLetter = 0x2000000
	// [\t]
	TabLetter = 0x4000000
	// ["]
	QuotLetter = 0x8000000
	// [`]
	BacktickLetter = 0x10000000
	// [!]
	ExclamaLetter = 0x20000000
)

const (
	ALPHA = UpperLetter | LowerLetter
)

var table = []uint32{
	0,                        //   [0]
	0,                        //   [1]
	0,                        //   [2]
	0,                        //   [3]
	0,                        //   [4]
	0,                        //   [5]
	0,                        //   [6]
	0,                        //   [7]
	0,                        //   [8]
	TabLetter,                //   [9]
	Newline,                  //   [10]
	0,                        //   [11]
	0,                        //   [12]
	Newline,                  //   [13]
	0,                        //   [14]
	0,                        //   [15]
	0,                        //   [16]
	0,                        //   [17]
	0,                        //   [18]
	0,                        //   [19]
	0,                        //   [20]
	0,                        //   [21]
	0,                        //   [22]
	0,                        //   [23]
	0,                        //   [24]
	0,                        //   [25]
	0,                        //   [26]
	0,                        //   [27]
	0,                        //   [28]
	0,                        //   [29]
	0,                        //   [30]
	0,                        //   [31]
	SpaceLetter,              //   [32]
	ExclamaLetter,            // ! [33]
	QuotLetter,               // " [34]
	0,                        // # [35]
	0,                        // $ [36]
	PercentLetter,            // % [37]
	AndLetter,                // & [38]
	0,                        // ' [39]
	0,                        // ( [40]
	0,                        // ) [41]
	Asterisk,                 // * [42]
	AddLetter,                // + [43]
	CommaLetter,              // , [44]
	SubLetter,                // - [45]
	DotLetter,                // . [46]
	SlantLetter,              // / [47]
	Digital | HexDigital,     // 0 [48]
	Digital | HexDigital,     // 1 [49]
	Digital | HexDigital,     // 2 [50]
	Digital | HexDigital,     // 3 [51]
	Digital | HexDigital,     // 4 [52]
	Digital | HexDigital,     // 5 [53]
	Digital | HexDigital,     // 6 [54]
	Digital | HexDigital,     // 7 [55]
	Digital | HexDigital,     // 8 [56]
	Digital | HexDigital,     // 9 [57]
	ColonLetter,              // : [58]
	SemiColonLetter,          // ; [59]
	LtLetter,                 // < [60]
	EqLetter,                 // = [61]
	GtLetter,                 // > [62]
	0,                        // ? [63]
	0,                        // @ [64]
	UpperLetter | HexDigital, // A [65]
	UpperLetter | HexDigital, // B [66]
	UpperLetter | HexDigital, // C [67]
	UpperLetter | HexDigital, // D [68]
	UpperLetter | HexDigital, // E [69]
	UpperLetter | HexDigital, // F [70]
	UpperLetter,              // G [71]
	UpperLetter,              // H [72]
	UpperLetter,              // I [73]
	UpperLetter,              // J [74]
	UpperLetter,              // K [75]
	UpperLetter,              // L [76]
	UpperLetter,              // M [77]
	UpperLetter,              // N [78]
	UpperLetter,              // O [79]
	UpperLetter,              // P [80]
	UpperLetter,              // Q [81]
	UpperLetter,              // R [82]
	UpperLetter,              // S [83]
	UpperLetter,              // T [84]
	UpperLetter,              // U [85]
	UpperLetter,              // V [86]
	UpperLetter,              // W [87]
	UpperLetter,              // X [88]
	UpperLetter,              // Y [89]
	UpperLetter,              // Z [90]
	0,                        // [ [91]
	RSlantLetter,             // \ [92]
	0,                        // ] [93]
	0,                        // ^ [94]
	Underline,                // _ [95]
	BacktickLetter,           // ` [96]
	LowerLetter | HexDigital, // a [97]
	LowerLetter | HexDigital, // b [98]
	LowerLetter | HexDigital, // c [99]
	LowerLetter | HexDigital, // d [100]
	LowerLetter | HexDigital, // e [101]
	LowerLetter | HexDigital, // f [102]
	LowerLetter,              // g [103]
	LowerLetter,              // h [104]
	LowerLetter,              // i [105]
	LowerLetter,              // j [106]
	LowerLetter,              // k [107]
	LowerLetter,              // l [108]
	LowerLetter,              // m [109]
	NLetter | LowerLetter,    // n [110]
	LowerLetter,              // o [111]
	LowerLetter,              // p [112]
	LowerLetter,              // q [113]
	RLetter | LowerLetter,    // r [114]
	LowerLetter,              // s [115]
	TLetter | LowerLetter,    // t [116]
	LowerLetter,              // u [117]
	LowerLetter,              // v [118]
	WLetter | LowerLetter,    // w [119]
	LowerLetter,              // x [120]
	LowerLetter,              // y [121]
	LowerLetter,              // z [122]
	0,                        // { [123]
	OrLetter,                 // | [124]
	0,                        // } [125]
	0,                        // ~ [126]
	0,                        // del [127]
}

func apiName(service, method, path, host, params string, maxApiLevel int, apiName string) (api string) {
	return apiWithParams(service, method, path, host, params, maxApiLevel)
}

func genXlogTags(service string, xlogs []string, respLength int64) []string {
	var tags []string
	switch service {
	case "UP":
		if stringsContain(xlogs, []string{"up.pop", "up.transform"}) {
			tags = append(tags, "fop")
		}
		if stringsContain(xlogs, []string{"UP.CB"}) {
			tags = append(tags, "callback")
		}
		if stringsContain(xlogs, []string{"lcy"}) {
			tags = append(tags, "lcy")
		}
	case "IO":
		if stringsContain(xlogs, []string{"io.op", "io.rop", "io.pop"}) {
			tags = append(tags, "fop")
		}
		if stringsContain(xlogs, []string{"gS.h"}) {
			tags = append(tags, "mirror")
		}
		if stringsContain(xlogs, []string{"IO.CB"}) {
			tags = append(tags, "callback")
		}
		if stringsContain(xlogs, []string{"AZF"}) && respLength > 1 {
			tags = append(tags, "allzero")
		}
	}
	return tags
}

func stringsContain(ss []string, subss []string) bool {
	for _, str := range ss {
		for _, substr := range subss {
			if strings.Contains(str, substr) {
				return true
			}
		}
	}
	return false
}

func sortAndUniq(ss []string) []string {
	length := len(ss)
	if length <= 1 {
		return ss
	}
	sort.Strings(ss)
	ret := []string{ss[0]}
	for i := 1; i < length; i++ {
		if ss[i] != ss[i-1] {
			ret = append(ret, ss[i])
		}
	}
	return ret
}

func hitXWarns(respXWarns, needXWarns []string) []string {
	var hits []string
	for _, r := range respXWarns {
		for _, n := range needXWarns {
			if r == n {
				hits = append(hits, r)
			}
		}
	}
	return hits
}

func is(typeMask uint32, c rune) bool {
	if uint(c) < uint(len(table)) {
		return (typeMask & table[c]) != 0
	}
	return false
}

func isType(typeMask uint32, str string) bool {
	if str == "" {
		return false
	}
	for _, c := range str {
		if !is(typeMask, c) {
			return false
		}
	}
	return true
}
