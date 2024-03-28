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
	"testing"
)

func TestCliUtilAlign(t *testing.T) {
	rows := table{
		arow("C1", "C2", "Cccc10", "none"),
		arow("name", "", 1<<40, ""),
		arow("for-bar", struct{ s string }{"struct"}, 111.111, ""),
		arow("bool", true, false, "1"),
	}
	t.Log("\n" + alignTable(rows...))
	t.Log("\n" + alignTableSep("\t", rows...))
	t.Log("\n" + alignTableSep(" -- ", rows...))

	columns := table{
		arow("String", "ccccccccccccccccccc"),
		arow("Int", 111),
		arow("Float", 111.111),
		arow("Any", struct{}{}),
		arow("Nil", nil),
	}
	t.Log("\n" + alignColumn(columns...))

	structs := []struct {
		Name   string
		Int    int
		Float  float32
		String string
		Any    interface{}
	}{
		{"name-1", 11, 19383.9, "ssssss", struct{ I int }{38333999}},
		{"name-2", 22, 0.0, "oooooooooooooooo", t.Log},
		{"", 0, 0.0, "", nil},
	}
	var all string
	for idx, s := range structs {
		idxColumns := table{
			arow("Name", s.Name),
			arow("Int", s.Int),
			arow("Float", s.Float),
			arow("String", s.String),
			arow("Any", s.Any),
		}
		all += alignColumnIndex(idx, idxColumns...)
	}
	t.Log("\n" + all)
}
