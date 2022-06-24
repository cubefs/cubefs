package common

import (
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strings"
)

type ColumnValue struct {
	Value interface{}
	Width int
}

func (cv *ColumnValue) SetValue(val interface{}) {
	cv.Value = val
}

type ColumnValues []ColumnValue

func (cv *ColumnValues) Add(v ...ColumnValue) {
	*cv = append(*cv, v...)
}

func (cv *ColumnValues) BuildColumnText() string {
	var formatBuilder = strings.Builder{}
	var values []interface{}
	for _, v := range *cv {
		if formatBuilder.Len() > 0 {
			formatBuilder.WriteString("  ")
		}
		if v.Width > 0 {
			formatBuilder.WriteString(fmt.Sprintf("%%-%dv", v.Width))
		} else {
			formatBuilder.WriteString("%v")
		}
		if v.Value != nil {
			values = append(values, v.Value)
		} else {
			values = append(values, "")
		}
	}
	return fmt.Sprintf(formatBuilder.String(), values...)
}

func NewColumnValues(v ...ColumnValue) ColumnValues {
	var values = ColumnValues(make([]ColumnValue, 0))
	if len(v) > 0 {
		values.Add(v...)
	}
	return values
}

type LogFileName struct {
	seq   uint64 // 文件序号
	index uint64 // 起始index（log entry)
}

func (l *LogFileName) String() string {
	return fmt.Sprintf("%016x-%016x.log", l.seq, l.index)
}

func (l *LogFileName) ParseFrom(s string) bool {
	_, err := fmt.Sscanf(s, "%016x-%016x.log", &l.seq, &l.index)
	return err == nil
}

type nameSlice []LogFileName

func (s nameSlice) Len() int           { return len(s) }
func (s nameSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nameSlice) Less(i, j int) bool { return s[i].seq < s[j].seq }

// 枚举目录下的所有日志文件并按序号排序
func ListLogEntryFiles(path string) (fnames []LogFileName, err error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	for _, name := range names {
		var n LogFileName
		if n.ParseFrom(name) {
			fnames = append(fnames, n)
		}
	}
	sort.Sort(nameSlice(fnames))
	return
}

var table = crc32.MakeTable(crc32.Castagnoli)

// CRC is a CRC-32 checksum computed using Castagnoli's polynomial.
type CRC uint32

// NewCRC creates a new crc based on the given bytes.
func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

// Update updates the crc with the given bytes.
func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

// Value returns a masked crc.
func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}