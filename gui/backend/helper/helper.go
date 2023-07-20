package helper

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
)

// Must make error panic.
func Must(err error, ctxinfo ...interface{}) {
	if err == nil {
		return
	}
	if len(ctxinfo) > 0 {
		var info []string
		for _, a := range ctxinfo {
			info = append(info, fmt.Sprintf("%v", a))
		}
		panic(fmt.Errorf("%v: %+v", strings.Join(info, " "), err))
	} else {
		panic(err)
	}
}

func CheckIpPort(s string) error {
	if s == "" {
		return fmt.Errorf("nil address")
	}
	idx := strings.Index(s, ":")
	if idx <= 0 {
		return fmt.Errorf("ip: %v 缺少端口号", s)
	}
	ip := net.ParseIP(s[:idx])
	if ip == nil {
		return fmt.Errorf("ip: %v 不合法", s)
	}
	return nil
}

func Percentage(used, total uint64) string {
	if used == 0 && total == 0 {
		return "0%"
	}
	ratio := (float64(used) * 100) / float64(total)
	if ratio < 1 && used > 0 {
		if ratio <= 0.1 {
			ratio = 0.1
		}
		return fmt.Sprintf("%.1f%%", ratio)
	}
	return fmt.Sprintf("%.0f%%", ratio)
}

func ByteConversion(fileSize uint64) (size string) {
	if fileSize < 1024 {
		return fmt.Sprintf("%.0fB", float64(fileSize)/float64(1))
	} else if fileSize < (1024 * 1024) {
		return fmt.Sprintf("%.0fKB", float64(fileSize)/float64(1024))
	} else if fileSize < (1024 * 1024 * 1024) {
		return fmt.Sprintf("%.0fMB", float64(fileSize)/float64(1024*1024))
	} else if fileSize < (1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%.0fGB", float64(fileSize)/float64(1024*1024*1024))
	} else if fileSize < (1024 * 1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%.1fTB", float64(fileSize)/float64(1024*1024*1024*1024))
	} else if fileSize < (1024 * 1024 * 1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%.1fPB", float64(fileSize)/float64(1024*1024*1024*1024*1024))
	} else {
		return fmt.Sprintf("%.1fEB", float64(fileSize)/float64(1024*1024*1024*1024*1024*1024))
	}
}

func GBByteConversion(size uint64) string {
	if size < 1024 {
		return fmt.Sprintf("%.0fGB", float64(size)/float64(1))
	} else if size < (1024 * 1024) {
		return fmt.Sprintf("%.1fTB", float64(size)/float64(1024))
	} else if size < (1024 * 1024 * 1024) {
		return fmt.Sprintf("%.1fPB", float64(size)/float64(1024*1024))
	} else {
		return fmt.Sprintf("%.1fEB", float64(size)/float64(1024*1024*1024))
	}
}

func BuildUrlParams(p interface{}) string {
	if p == nil {
		return ""
	}
	t := reflect.TypeOf(p)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return ""
	}

	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	params := make([]string, 0)
	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i).Tag.Get("json")
		if strings.Contains(key, "omitempty") {
			key = key[:len(key)-len(",omitempty")]
			if v.Field(i).IsNil() {
				continue
			}
		}
		val := v.Field(i).Interface()
		if v.Field(i).Kind() == reflect.Ptr {
			val = v.Field(i).Elem().Interface()
		}
		params = append(params, fmt.Sprintf("%s=%v", key, val))
	}
	return strings.Join(params, "&")
}

func GetIp(s string) string {
	if idx := strings.Index(s, "//"); idx >= 0 {
		s = s[idx+2:]
	}
	index := strings.Index(s, ":")
	if index < 0 {
		return s
	}
	return s[:index]
}

func FloatRound(f float64, n int) float64 {
	format := "%." + strconv.Itoa(n) + "f"
	res, _ := strconv.ParseFloat(fmt.Sprintf(format, f), 64)
	return res
}
