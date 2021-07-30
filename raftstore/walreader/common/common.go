package common

import (
	"fmt"
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
