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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func join(lines []string) string {
	return strings.Join(lines, "\n")
}

func TestTableFmtTableAppend(t *testing.T) {
	tbl := Table{NewRow("Header1", "Header2")}
	tbl = tbl.Append(NewRow("Value1", "Value2"))
	tbl = tbl.Append(NewRow("Value3", "Value4")).Append(NewRow("5", "6"))

	result := Align(tbl...)
	t.Log("\n" + join(result))
}

func TestTableFmtAlign(t *testing.T) {
	rows := Table{
		NewRow("C1", "C2", "C3", "C4"),
		NewRow("name", "-", 1<<40, "-"),
		NewRow("for-bar", struct{ s string }{"struct"}, 111.111, ""),
		NewRow("bool", true, false, "1"),
	}
	t.Log("\n" + join(Align(rows...)))
	t.Log("\n" + join(AlignSep("\t", rows...)))
	t.Log("\n" + join(AlignSep(" * ", rows...)))
}

func TestTableFmtAlignWith(t *testing.T) {
	rows := Table{
		NewRow("Name", "Age", "Score"),
		NewRow("Alice", 25, 95.5),
		NewRow("Bob", 30, 88.0),
		NewRow("Charlie", 22, 100.0),
	}

	t.Log("All left (default):\n" + join(Align(rows...)))

	aligns := []Alignment{AlignLeft, AlignRight}
	t.Log("Name left, numbers right:\n" + join(AlignWith(aligns, rows...)))

	aligns2 := []Alignment{AlignCenter, AlignCenter, AlignRight}
	t.Log("Name center, age center, score right:\n" + join(AlignWith(aligns2, rows...)))
}

func TestTableFmtAlignColumn(t *testing.T) {
	columns := Table{
		NewRow("String", "ccccccccccccccccccc"),
		NewRow("Int", 111),
		NewRow("Float", 111.111),
		NewRow("Any", struct{}{}),
		NewRow("Nil", nil),
	}
	t.Log("\n" + join(AlignColumn(columns...)))
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
	var all []string
	for idx, s := range structs {
		idxColumns := Table{
			NewRow("Name", s.Name),
			NewRow("Int", s.Int),
			NewRow("Float", s.Float),
			NewRow("String", s.String),
			NewRow("Any", s.Any),
		}
		all = append(all, AlignColumnIndex(idx+1, idxColumns...)...)
	}
	t.Log("\n" + join(all))
}

func TestTableFmtAlignColumnIndent(t *testing.T) {
	rows := Table{
		NewRow("Key1", "Value1"),
		NewRow("Key2", "Value2"),
		NewRow("Key3", "Value3"),
	}
	result := AlignColumnIndent("*", rows...)
	t.Log("\n" + join(result))
}

func TestTableFmtSummary(t *testing.T) {
	rows := Table{
		NewRow("IDC", "Count", "Used", "Free"),
		NewRow("idc1", 100, "1.5 TiB", "500 GiB"),
		NewRow("idc2", 200, "3.0 TiB", "1.0 TiB"),
		NewRow("TOTAL", 300, "4.5 TiB", "1.5 TiB"),
	}

	lines := AlignWith([]Alignment{AlignRight}, rows...)
	summary := Summary(lines)
	t.Log("\n" + join(summary))

	t.Log("\nWith indent:")
	for _, line := range summary {
		t.Log("\t * " + line)
	}
}

func TestTableFmtSummaryLessThanThreeRows(t *testing.T) {
	require.Nil(t, Summary(nil))
	for _, lines := range [][]string{
		{},
		{"Header"},
		{"Header", "Data"},
	} {
		require.Equal(t, lines, Summary(lines))
	}
}
