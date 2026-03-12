// Copyright 2026 The CubeFS Authors.
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

package tablefmt

import (
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util"
)

// Alignment defines column alignment.
type Alignment int

const (
	AlignLeft Alignment = iota
	AlignRight
	AlignCenter
)

type Row []any

func NewRow(values ...any) Row { return values }

type Table []Row

func (t Table) Append(rows ...Row) Table { return append(t, rows...) }

func str2Any(strs []string) []any {
	r := make([]any, len(strs))
	for idx := range strs {
		r[idx] = strs[idx]
	}
	return r
}

func rowStr(r Row) []string {
	s := make([]string, len(r))
	for idx := range s {
		s[idx] = util.Any2String(r[idx])
	}
	return s
}

func computeWidths(rows []Row) ([]int, [][]string) {
	table := make([][]string, len(rows))
	widths := make([]int, len(rows[0]))
	for idx, row := range rows {
		str := rowStr(row)
		table[idx] = str
		for ii, s := range str {
			if l := len(s); l > widths[ii] {
				widths[ii] = l
			}
		}
	}
	return widths, table
}

func centerPad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	pad := width - len(s)
	left := pad / 2
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", pad-left)
}

func makePattern(widths []int, aligns []Alignment, table [][]string) []string {
	pattern := make([]string, len(widths))
	align := AlignLeft
	for i, w := range widths {
		if i < len(aligns) {
			align = aligns[i]
		}
		switch align {
		case AlignRight:
			pattern[i] = fmt.Sprintf("%%%ds", w)
		case AlignCenter:
			for row := range table {
				table[row][i] = centerPad(table[row][i], w)
			}
			pattern[i] = "%s"
		default:
			pattern[i] = fmt.Sprintf("%%-%ds", w)
		}
	}
	return pattern
}

// AlignSepWith aligns rows with custom separator and column alignments.
func AlignSepWith(sep string, aligns []Alignment, rows ...Row) string {
	widths, table := computeWidths(rows)
	pattern := makePattern(widths, aligns, table)
	pt := strings.Join(pattern, sep) + "\n"
	sb := strings.Builder{}
	for _, row := range table {
		sb.WriteString(fmt.Sprintf(pt, str2Any(row)...))
	}
	return sb.String()
}

// AlignSep aligns rows with a custom separator (all columns left-aligned).
func AlignSep(sep string, rows ...Row) string {
	return AlignSepWith(sep, nil, rows...)
}

// Align aligns rows as a table with " | " separator.
func Align(rows ...Row) string {
	return AlignSep(" | ", rows...)
}

// AlignWith aligns rows with specified column alignments.
func AlignWith(aligns []Alignment, rows ...Row) string {
	return AlignSepWith(" | ", aligns, rows...)
}

// AlignColumn aligns rows as key-value pairs with " : " separator.
func AlignColumn(rows ...Row) string {
	return AlignSep(" : ", rows...)
}

// AlignColumnIndent aligns rows with indentation prefix.
func AlignColumnIndent(indent string, rows ...Row) string {
	widths, table := computeWidths(rows)
	pattern := makePattern(widths, nil, table)
	pt := strings.Join(pattern, " : ") + "\n"
	sb := strings.Builder{}
	for idx, row := range table {
		prefix := strings.Repeat(" ", len(indent)) + "   "
		if idx == 0 {
			prefix = indent + " - "
		}
		sb.WriteString(prefix + fmt.Sprintf(pt, str2Any(row)...))
	}
	return sb.String()
}

// AlignColumnIndex aligns rows with numeric index prefix.
func AlignColumnIndex(index int, rows ...Row) string {
	return AlignColumnIndent(util.Any2String(index), rows...)
}
