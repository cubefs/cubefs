package auditlog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Query struct {
	Must    []map[string]interface{} `json:"must"`
	MustNot []map[string]interface{} `json:"must_not"`
	Should  []map[string]interface{} `json:"should"`
	Minimum int                      `json:"minimum_should_match"`
}

type Log struct {
	ReqType    string `json:"req_type"`
	Module     string `json:"module"`
	StartTime  int64  `json:"start_time"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	ReqHeader  M      `json:"req_header"`
	ReqParams  string `json:"req_params"`
	StatusCode int    `json:"status_code"`
	RespHeader M      `json:"resp_header"`
	RespBody   string `json:"resp_body"`
	RespLength int64  `json:"resp_length"`
	Duration   int64  `json:"duration"`
}

func FilterLog(log Log, filter Query) bool {
	for _, clause := range filter.Must {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if !isEqual(logFieldValue(log, field), val) {
							return false
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if !strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							return false
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						rangeMap := val.(map[string]interface{})
						fieldVal := logFieldValue(log, field).(int64)
						_, ok1 := rangeMap["gte"]
						_, ok2 := rangeMap["lte"]
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal < gte {
									return false
								}
							} else {
								fmt.Println("rangeMap[\"gte\"]包含非数字")
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal > lte {
									return false
								}
							} else {
								fmt.Println("rangeMap[\"lte\"]包含非数字")
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							fmt.Printf("正则表达式错误 %s = %v\n", field, val)
							return false
						}
						if !reg.MatchString(logFieldValue(log, field).(string)) {
							return false
						}
					}
				}
			case "bool":
				que := Query{}
				arr, err := json.Marshal(clause["bool"])
				if err != nil {
					fmt.Println(err)
				}
				err = json.Unmarshal(arr, &que)
				if err != nil {
					fmt.Println(err)
				}
				if !FilterLog(log, que) {
					return false
				}
			default:
				fmt.Println("Unsupported filter type:", key)
				return false
			}
		}
	}
	for _, clause := range filter.MustNot {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if isEqual(logFieldValue(log, field), val) {
							return false
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							return false
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						rangeMap := val.(map[string]interface{})
						fieldVal := logFieldValue(log, field).(int64)
						_, ok1 := rangeMap["gte"]
						_, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							if isDigit(rangeMap["gte"].(string)) && isDigit(rangeMap["lte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal >= gte && fieldVal <= lte {
									return false
								}
							} else {
								fmt.Println("rangeMap[\"gte\"]包含非数字")
							}
						}
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal >= gte {
									return false
								}
							} else {
								fmt.Println("rangeMap[\"gte\"]包含非数字")
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal <= lte {
									return false
								}
							} else {
								fmt.Println("rangeMap[\"lte\"]包含非数字")
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							fmt.Printf("正则表达式错误 %s = %v\n", field, val)
							return false
						}
						if reg.MatchString(logFieldValue(log, field).(string)) {
							return false
						}
					}
				}
			case "bool":
				que := Query{}
				arr, err := json.Marshal(clause["bool"])
				if err != nil {
					fmt.Println(err)
				}
				err = json.Unmarshal(arr, &que)
				if err != nil {
					fmt.Println(err)
				}
				if !FilterLog(log, que) {
					return false
				}
			default:
				fmt.Println("Unsupported filter type:", key)
				return false
			}
		}
	}
A:
	for _, clause := range filter.Should {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if isEqual(logFieldValue(log, field), val) {
							break A
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						if strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							break A
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						rangeMap := val.(map[string]interface{})
						fieldVal := logFieldValue(log, field).(int64)
						_, ok1 := rangeMap["gte"]
						_, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							if isDigit(rangeMap["gte"].(string)) && isDigit(rangeMap["lte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal >= gte && fieldVal <= lte {
									break A
								}
							} else {
								fmt.Println("rangeMap[\"gte\"]包含非数字")
							}
						}
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal >= gte {
									break A
								}
							} else {
								fmt.Println("rangeMap[\"gte\"]包含非数字")
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal <= lte {
									break A
								}
							} else {
								fmt.Println("rangeMap[\"lte\"]包含非数字")
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fmt.Printf("Filtering by %s = %v\n", field, val)
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							fmt.Printf("正则表达式错误 %s = %v\n", field, val)
							return false
						}
						if reg.MatchString(logFieldValue(log, field).(string)) {
							break A
						}
					}
				}
			case "bool":
				que := Query{}
				arr, err := json.Marshal(clause["bool"])
				if err != nil {
					fmt.Println(err)
				}
				err = json.Unmarshal(arr, &que)
				if err != nil {
					fmt.Println(err)
				}
				if FilterLog(log, que) {
					break A
				}
			default:
				fmt.Println("Unsupported filter type:", key)
				return false
			}
		}
		return false
	}
	return true
}

func logFieldValue(log Log, field string) interface{} {
	switch field {
	case "req_type":
		return log.ReqType
	case "module":
		return log.Module
	case "start_time":
		return log.StartTime
	case "method":
		return log.Method
	case "path":
		return log.Path
	case "req_header":
		return string(log.ReqHeader.Encode())
	case "req_params":
		return log.ReqParams
	case "status_code":
		return log.StatusCode
	case "resp_header":
		return string(log.RespHeader.Encode())
	case "resp_body":
		return log.RespBody
	case "resp_length":
		return log.RespLength
	case "duration":
		return log.Duration
	default:
		fmt.Println("Unsupported field:", field)
		return nil
	}
}

func isDigit(str string) bool {
	for _, x := range []rune(str) {
		if !unicode.IsDigit(x) {
			return false
		}
	}
	return true
}

func getDateTime(str string) (time.Time, error) {
	switch len(str) {
	case len("2006-01-02 15:04:05"):
		return time.Parse("2006-01-02 15:04:05", str)
	case len("2006-01-02"):
		return time.Parse("2006-01-02", str)
	default:
		fmt.Println("Unsupported Time:", str)
		return time.Parse("2006-01-02", str)
	}
}

func isEqual(a, b interface{}) bool {
	switch a := a.(type) {
	case int:
		if b, ok := b.(int); ok {
			return a == b
		}
		if b, ok := b.(int64); ok {
			return int64(a) == b
		}
		if b, ok := b.(float32); ok {
			return float32(a) == b
		}
		if b, ok := b.(float64); ok {
			return float64(a) == b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a == b
		}
		if b, ok := b.(int); ok {
			return a == int64(b)
		}
		if b, ok := b.(float32); ok {
			return float32(a) == b
		}
		if b, ok := b.(float64); ok {
			return float64(a) == b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a == b
		}
		if b, ok := b.(int); ok {
			return a == float32(b)
		}
		if b, ok := b.(int64); ok {
			return a == float32(b)
		}
		if b, ok := b.(float64); ok {
			return float64(a) == b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a == b
		}
		if b, ok := b.(int); ok {
			return a == float64(b)
		}
		if b, ok := b.(int64); ok {
			return a == float64(b)
		}
		if b, ok := b.(float32); ok {
			return float32(a) == b
		}
	case string:
		if b, ok := b.(string); ok {
			return a == b
		}
	default:
		return false
	}
	return false
}
