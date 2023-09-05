// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package auditlog

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

//Query Define a struct of log filter
//This is a example of Query
//{
//	MustNot []struct {
//		Match []struct {
//			Path string `json:"path"`
//		} `json:"match,omitempty"`
//		Range []struct {
//			StartTime struct {
//				Gte string `json:"gte"`
//			} `json:"start_time"`
//		} `json:"range,omitempty"`
//	} `json:"must_not"`
//	Must []struct {
//		Term []struct {
//			Module string `json:"module"`
//			Method string `json:"method"`
//		} `json:"term,omitempty"`
//		Regexp []struct {
//			ReqHeader string `json:"req_header"`
//		} `json:"regexp,omitempty"`
//		Match []struct {
//			Path      string `json:"path"`
//			ReqHeader string `json:"req_header"`
//		} `json:"match,omitempty"`
//		Bool struct {
//			Should []struct {
//				Term []struct {
//					StatusCode int `json:"status_code"`
//					RespLength int `json:"resp_length,omitempty"`
//				} `json:"term"`
//			} `json:"should"`
//		} `json:"bool,omitempty"`
//	} `json:"must"`
//}

type Query struct {
	//Must is the rule of
	Must     []map[string]interface{} `json:"must"`
	MustNot  []map[string]interface{} `json:"must_not"`
	Should   []map[string]interface{} `json:"should"`
	priority int
	index    int
}

type PriorityQueue []*Query

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Query)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type Log AuditLog

var priorityQueue PriorityQueue

func (filter Query) Init() {
	queryList := make([]*Query, 0)
	var index int = 0
	for _, clause := range filter.Must {
		tem := new(Query)
		tt := make([]map[string]interface{}, 0)
		tt = append(tt, clause)
		tem.Must = tt
		tem.priority = 0
		tem.index = index
		index++
		queryList = append(queryList, tem)
	}
	for _, clause := range filter.MustNot {
		tem := new(Query)
		tt := make([]map[string]interface{}, 1)
		tt = append(tt, clause)
		tem.MustNot = tt
		tem.priority = 0
		tem.index = index
		index++
		queryList = append(queryList, tem)
	}
	if filter.Should == nil {
		tem := new(Query)
		tem.Should = filter.Should
		tem.priority = 0
		tem.index = index
		index++
		queryList = append(queryList, tem)
	}
	priorityQueue = PriorityQueue(queryList)
	heap.Init(&priorityQueue)
}

type FilterError struct {
	msg string
}

func (e FilterError) Error() string {
	return fmt.Sprintf("msg:%v", e.msg)
}

func FilterLogWithPriority(log Log) bool {
	lenPriorityQueue := priorityQueue.Len()
	for i := 0; i < lenPriorityQueue; i++ {
		if bo, _ := FilterLog(log, *priorityQueue[i]); !bo {
			priorityQueue[i].priority += 1
			heap.Fix(&priorityQueue, i)
			return false
		}
	}
	return true
}

func FilterLog(log Log, filter Query) (bool, error) {
	for _, clause := range filter.Must {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						if !isEqual(logFieldValue(log, field), val) {
							return false, nil
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						if !strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							return false, nil
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						rangeMap := val.(map[string]interface{})
						fieldVal := logFieldValue(log, field).(int64)
						_, ok1 := rangeMap["gte"]
						_, ok2 := rangeMap["lte"]
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal < gte {
									return false, nil
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal > lte {
									return false, nil
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							return false, err
						}
						if !reg.MatchString(logFieldValue(log, field).(string)) {
							return false, err
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
				if ok, err := FilterLog(log, que); !ok {
					return false, err
				}
			default:
				return false, nil
			}
		}
	}
	for _, clause := range filter.MustNot {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						if isEqual(logFieldValue(log, field), val) {
							return false, nil
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						if strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							return false, nil
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						rangeMap := val.(map[string]interface{})
						fieldVal := logFieldValue(log, field).(int64)
						_, ok1 := rangeMap["gte"]
						_, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							if isDigit(rangeMap["gte"].(string)) && isDigit(rangeMap["lte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal >= gte && fieldVal <= lte {
									return false, nil
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal >= gte {
									return false, nil
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal <= lte {
									return false, nil
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							return false, err
						}
						if reg.MatchString(logFieldValue(log, field).(string)) {
							return false, err
						}
					}
				}
			case "bool":
				que := Query{}
				arr, err := json.Marshal(clause["bool"])
				if err != nil {

				}
				err = json.Unmarshal(arr, &que)
				if err != nil {
					fmt.Println(err)
				}
				if ok, err := FilterLog(log, que); !ok {
					return false, err
				}
			default:
				return false, nil
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
						if isEqual(logFieldValue(log, field), val) {
							break A
						}
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						if strings.Contains(logFieldValue(log, field).(string), val.(string)) {
							break A
						}
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
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
								return false, FilterError{msg: "range error"}
							}
						}
						if ok1 && !ok2 {
							if isDigit(rangeMap["gte"].(string)) {
								gte, _ := strconv.ParseInt(rangeMap["gte"].(string), 10, 64)
								if fieldVal >= gte {
									break A
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
						if !ok1 && ok2 {
							if isDigit(rangeMap["lte"].(string)) {
								lte, _ := strconv.ParseInt(rangeMap["lte"].(string), 10, 64)
								if fieldVal <= lte {
									break A
								}
							} else {
								return false, FilterError{msg: "range error"}
							}
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						reg, err := regexp.Compile(val.(string))
						if err != nil {
							return false, err
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
				if ok, _ := FilterLog(log, que); !ok {
					break A
				}
			default:
				return false, FilterError{msg: "without filter " + key}
			}
		}
		return false, nil
	}
	return true, nil
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
