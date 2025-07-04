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

func str2Any(strs []string) []interface{} {
	r := make([]interface{}, len(strs))
	for idx := range strs {
		r[idx] = strs[idx]
	}
	return r
}

func alignTableSep(sep string, rows ...[]interface{}) string {
	pattern, table := align(rows)
	pt := strings.Join(pattern, sep) + "\n"
	sb := strings.Builder{}
	for idx := range table {
		sb.WriteString(fmt.Sprintf(pt, str2Any(table[idx])...))
	}
	return sb.String()
}

func alignColumn(rows ...[]interface{}) string {
	return alignTableSep(" : ", rows...)
}

func arow(r ...interface{}) []interface{} {
	return r
}

func alignTable(rows ...[]interface{}) string {
	return alignTableSep(" | ", rows...)
}
