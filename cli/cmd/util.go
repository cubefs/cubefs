// Copyright 2024 The CubeFS Authors.
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
	"strings"

	"github.com/cubefs/cubefs/util"
)

type table [][]interface{}

func (t table) append(rows ...[]interface{}) table {
	return append(t, rows...)
}

func arow(r ...interface{}) []interface{} {
	return r
}

func str2Any(strs []string) []interface{} {
	r := make([]interface{}, len(strs))
	for idx := range strs {
		r[idx] = strs[idx]
	}
	return r
}

func rowStr(r []interface{}) []string {
	s := make([]string, len(r))
	for idx := range s {
		s[idx] = util.Any2String(r[idx])
	}
	return s
}

func align(rows [][]interface{}) (pattern []string, table [][]string) {
	table = make([][]string, len(rows))
	lens := make([]int, len(rows[0]))
	for idx := range table {
		str := rowStr(rows[idx])
		for ii := range str {
			if l := len(str[ii]); l > lens[ii] {
				lens[ii] = l
			}
		}
		table[idx] = rowStr(rows[idx])
	}
	pattern = make([]string, len(rows[0]))
	for idx := range lens {
		pattern[idx] = fmt.Sprintf("%%-%ds", lens[idx])
	}
	return
}

// alignTableSep align rows table with separator.
func alignTableSep(sep string, rows ...[]interface{}) string {
	pattern, table := align(rows)
	pt := strings.Join(pattern, sep) + "\n"
	sb := strings.Builder{}
	for idx := range table {
		sb.WriteString(fmt.Sprintf(pt, str2Any(table[idx])...))
	}
	return sb.String()
}

// alignTable align rows table with two blanks.
func alignTable(rows ...[]interface{}) string {
	return alignTableSep(" | ", rows...)
}

// alignColumn align struct.
func alignColumn(rows ...[]interface{}) string {
	return alignTableSep(" : ", rows...)
}

// alignColumnIndent align struct with indentation.
func alignColumnIndent(indent string, rows ...[]interface{}) string {
	pattern, table := align(rows)
	pt := strings.Join(pattern, " : ") + "\n"
	sb := strings.Builder{}
	first := true
	genIndent := func() string {
		if first {
			first = false
			return indent + "- "
		}
		return indent + "  "
	}
	for idx := range table {
		sb.WriteString(genIndent() + fmt.Sprintf(pt, str2Any(table[idx])...))
	}
	return sb.String()
}

// alignColumnIndex align struct with index.
func alignColumnIndex(index int, rows ...[]interface{}) string {
	return alignColumnIndent(util.Any2String(index), rows...)
}
