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
	rows := [][]interface{}{
		row("C1", "C2", "Cccc10", "none"),
		row("name", "", 1<<40, ""),
		row("for-bar", struct{ s string }{"struct"}, 111.111, ""),
		row("bool", true, false, "1"),
	}
	t.Log("\n" + alignTable(rows...))
	t.Log("\n" + alignTableSep("\t", rows...))
	t.Log("\n" + alignTableSep(" -- ", rows...))

	columns := [][]interface{}{
		row("String", "ccccccccccccccccccc"),
		row("Int", 111),
		row("Float", 111.111),
		row("Any", struct{}{}),
		row("Nil", nil),
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
	for idx, s := range structs {
		idxColumns := [][]interface{}{
			row("Name", s.Name),
			row("Int", s.Int),
			row("Float", s.Float),
			row("String", s.String),
			row("Any", s.Any),
		}
		t.Log("\n" + alignColumnIndex(idx, idxColumns...))
	}
}
