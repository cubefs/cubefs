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

import "testing"

func TestTableFmtTableAppend(t *testing.T) {
	tbl := Table{NewRow("Header1", "Header2")}
	tbl = tbl.Append(NewRow("Value1", "Value2"))
	tbl = tbl.Append(NewRow("Value3", "Value4")).Append(NewRow("5", "6"))

	result := Align(tbl...)
	t.Log("\n" + result)
}

func TestTableFmtAlign(t *testing.T) {
	rows := Table{
		NewRow("C1", "C2", "C3", "C4"),
		NewRow("name", "-", 1<<40, "-"),
		NewRow("for-bar", struct{ s string }{"struct"}, 111.111, ""),
		NewRow("bool", true, false, "1"),
	}
	t.Log("\n" + Align(rows...))
	t.Log("\n" + AlignSep("\t", rows...))
	t.Log("\n" + AlignSep(" * ", rows...))
}

func TestTableFmtAlignWith(t *testing.T) {
	rows := Table{
		NewRow("Name", "Age", "Score"),
		NewRow("Alice", 25, 95.5),
		NewRow("Bob", 30, 88.0),
		NewRow("Charlie", 22, 100.0),
	}

	t.Log("All left (default):\n" + Align(rows...))

	aligns := []Alignment{AlignLeft, AlignRight}
	t.Log("Name left, numbers right:\n" + AlignWith(aligns, rows...))

	aligns2 := []Alignment{AlignCenter, AlignCenter, AlignRight}
	t.Log("Name center, age center, score right:\n" + AlignWith(aligns2, rows...))
}

func TestTableFmtAlignColumn(t *testing.T) {
	columns := Table{
		NewRow("String", "ccccccccccccccccccc"),
		NewRow("Int", 111),
		NewRow("Float", 111.111),
		NewRow("Any", struct{}{}),
		NewRow("Nil", nil),
	}
	t.Log("\n" + AlignColumn(columns...))
}

func TestTableFmtAlignColumnIndex(t *testing.T) {
	structs := []struct {
		Name   string
		Int    int
		Float  float32
		String string
		Any    any
	}{
		{"name-1", 11, 19383.9, "ssssss", struct{ I int }{38333999}},
		{"name-2", 22, 0.0, "ooooooooooooo", t.Log},
		{"", 0, 0.0, "", nil},
	}
	var all string
	for idx, s := range structs {
		idxColumns := Table{
			NewRow("Name", s.Name),
			NewRow("Int", s.Int),
			NewRow("Float", s.Float),
			NewRow("String", s.String),
			NewRow("Any", s.Any),
		}
		all += AlignColumnIndex(idx+1, idxColumns...)
	}
	t.Log("\n" + all)
}

func TestTableFmtAlignColumnIndent(t *testing.T) {
	rows := Table{
		NewRow("Key1", "Value1"),
		NewRow("Key2", "Value2"),
		NewRow("Key3", "Value3"),
	}
	result := AlignColumnIndent("*", rows...)
	t.Log("\n" + result)
}
