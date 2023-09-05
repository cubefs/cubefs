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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Query Define a struct of log filter
// This is an example of Query

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
//	} `json:"must"`
//	Should []struct {
//		Match []struct {
//			Path string `json:"path"`
//		} `json:"match,omitempty"`
//		Term []struct {
//			StatusCode int `json:"status_code"`
//			RespLength int `json:"resp_length"`
//		} `json:"term,omitempty"`
//	} `json:"should"`
//}

var mutex sync.Mutex

type FilterFunc struct {
	t        string
	f        func(log *AuditLog) bool
	priority int64
	err      error
}

type Query struct {
	Must        []map[string]interface{} `json:"must"`
	MustNot     []map[string]interface{} `json:"must_not"`
	Should      []map[string]interface{} `json:"should"`
	all         PriorityQueue
	or          PriorityQueue
	priorityAll int64
	priorityOr  int64
	num         int64
	upper       int64
}

type PriorityQueue []*FilterFunc

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*FilterFunc)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

func (filter *Query) Init() {
	filter.num = 500
	filter.upper = 1000
	all := make([]*FilterFunc, 0)
	for _, clause := range filter.Must {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("term", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						rangeMap := val.(map[string]interface{})
						val1, ok1 := rangeMap["gte"]
						val2, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), fmt.Sprintf("%v", val1))
							re := FilterFunc{"must", fun, 0, err}
							all = append(all, &re)
						} else if ok1 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), "")
							re := FilterFunc{"must", fun, 0, err}
							all = append(all, &re)
						} else if ok2 {
							fun, err := parse("range", field, "", fmt.Sprintf("%v", val2))
							re := FilterFunc{"must", fun, 0, err}
							all = append(all, &re)
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			}
		}
	}
	for _, clause := range filter.MustNot {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("term", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						rangeMap := val.(map[string]interface{})
						val1, ok1 := rangeMap["gte"]
						val2, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), fmt.Sprintf("%v", val1))
							re := FilterFunc{"mustNot", fun, 0, err}
							all = append(all, &re)
						} else if ok1 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), "")
							re := FilterFunc{"mustNot", fun, 0, err}
							all = append(all, &re)
						} else if ok2 {
							fun, err := parse("range", field, "", fmt.Sprintf("%v", val2))
							re := FilterFunc{"mustNot", fun, 0, err}
							all = append(all, &re)
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			}
		}
	}
	or := make([]*FilterFunc, 0)
	for _, clause := range filter.Should {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("term", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"should", fun, 0, err}
						or = append(or, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"should", fun, 0, err}
						or = append(or, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						rangeMap := val.(map[string]interface{})
						val1, ok1 := rangeMap["gte"]
						val2, ok2 := rangeMap["lte"]
						if ok1 && ok2 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), fmt.Sprintf("%v", val1))
							re := FilterFunc{"should", fun, 0, err}
							all = append(all, &re)
						} else if ok1 {
							fun, err := parse("range", field, fmt.Sprintf("%v", val1), "")
							re := FilterFunc{"should", fun, 0, err}
							all = append(all, &re)
						} else if ok2 {
							fun, err := parse("range", field, "", fmt.Sprintf("%v", val2))
							re := FilterFunc{"should", fun, 0, err}
							all = append(all, &re)
						}
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val), "")
						re := FilterFunc{"should", fun, 0, err}
						or = append(or, &re)
					}
				}
			}
		}
	}
	filter.all = all
	filter.or = or
	heap.Init(&filter.all)
	heap.Init(&filter.or)
}

type FilterError struct {
	msg string
}

func (e FilterError) Error() string {
	return fmt.Sprintf("msg:%v", e.msg)
}

func (r *Query) updatePriority(a *PriorityQueue, i int) {
	mutex.Lock()
	defer mutex.Unlock()
	heap.Fix(a, i)
	r.num = 0
}

func (filter *Query) FilterLogWithPriority(log *AuditLog) (bool, error) {
	filter.num++
	if filter.priorityAll >= filter.priorityOr {
		lenPriorityQueue := filter.all.Len()
		for i := 0; i < lenPriorityQueue; i++ {
			tem := filter.all[i]
			if tem.t == "must" && !tem.f(log) {
				atomic.AddInt64(&(filter.priorityAll), 1)
				if filter.num >= filter.upper {
					atomic.AddInt64(&(tem.priority), 1)
					filter.updatePriority(&filter.all, i)
				}
				return false, tem.err
			} else if tem.t == "mustNot" && tem.f(log) {
				atomic.AddInt64(&(filter.priorityAll), 1)
				if filter.num >= filter.upper {
					atomic.AddInt64(&(tem.priority), 1)
					filter.updatePriority(&filter.all, i)
				}
				return false, tem.err
			}
		}
		lenPriorityQueue = filter.or.Len()
		if lenPriorityQueue == 0 {
			return true, nil
		}
		for i := 0; i < lenPriorityQueue; i++ {
			if filter.or[i].f(log) {
				if filter.num >= filter.upper {
					atomic.AddInt64(&(filter.or[i].priority), 1)
					filter.updatePriority(&filter.or, i)
				}
				return true, nil
			}
		}
		atomic.AddInt64(&(filter.priorityOr), 1)
		return false, FilterError{"Should filter "}
	} else {
		lenPriorityQueue := filter.or.Len()
		if lenPriorityQueue > 0 {
			flag := true
			for i := 0; i < lenPriorityQueue; i++ {
				if filter.or[i].f(log) {
					if filter.num >= filter.upper {
						atomic.AddInt64(&(filter.or[i].priority), 1)
						filter.updatePriority(&filter.or, i)
					}
					flag = false
					break
				}
			}
			if flag {
				atomic.AddInt64(&(filter.priorityOr), 1)
				return false, FilterError{"Should filter "}
			}
		}
		lenPriorityQueue = filter.all.Len()
		for i := 0; i < lenPriorityQueue; i++ {
			tem := filter.all[i]
			if tem.t == "must" && !tem.f(log) {
				atomic.AddInt64(&(filter.priorityAll), 1)
				if filter.num >= filter.upper {
					atomic.AddInt64(&(tem.priority), 1)
					filter.updatePriority(&filter.all, i)
				}
				return false, tem.err
			} else if tem.t == "mustNot" && tem.f(log) {
				atomic.AddInt64(&(filter.priorityAll), 1)
				if filter.num >= filter.upper {
					atomic.AddInt64(&(tem.priority), 1)
					filter.updatePriority(&filter.all, i)
				}
				return false, tem.err
			}
		}
		return true, nil
	}
}

func parse(operation, field, value, value2 string) (func(log *AuditLog) bool, error) {
	err := fmt.Errorf("unsupported (%s %s)", field, operation)
	switch field {
	case "req_type":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.ReqType == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.ReqType, value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.ReqType)
			}, err
		default:
			return nil, err
		}
	case "module":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.Module == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.Module, value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.Module)
			}, err
		default:
			return nil, err
		}
	case "start_time":
		val, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return nil, e
		}
		switch operation {
		case "range":
			val2, _ := strconv.ParseInt(value2, 10, 64)
			if value2 == "" {
				return func(log *AuditLog) bool {
					return log.StartTime >= val
				}, err
			} else if value == "" {
				return func(log *AuditLog) bool {
					return log.StartTime <= val2
				}, err
			} else {
				return func(log *AuditLog) bool {
					return log.StartTime >= val && log.StartTime <= val2
				}, err
			}
		case "term":
			return func(log *AuditLog) bool {
				return log.StartTime == val
			}, err
		default:
			return nil, err
		}
	case "method":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.Method == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.Method, value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.Method)
			}, err
		default:
			return nil, err
		}
	case "path":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.Path == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.Path, value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.Path)
			}, err
		default:
			return nil, err
		}
	case "req_header":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return string(log.ReqHeader.Encode()) == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(string(log.ReqHeader.Encode()), value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(string(log.ReqHeader.Encode()))
			}, err
		default:
			return nil, err
		}
	case "req_params":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.ReqParams == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.ReqParams, value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.ReqParams)
			}, err
		default:
			return nil, err
		}
	case "status_code":
		val, e := strconv.Atoi(value)
		if e != nil {
			return nil, e
		}
		switch operation {
		case "range":
			val2, _ := strconv.Atoi(value)
			if value2 == "" {
				return func(log *AuditLog) bool {
					return log.StatusCode >= val
				}, err
			} else if value == "" {
				return func(log *AuditLog) bool {
					return log.StatusCode <= val2
				}, err
			} else {
				return func(log *AuditLog) bool {
					return log.StatusCode >= val && log.StatusCode <= val2
				}, err
			}
		case "term":
			return func(log *AuditLog) bool {
				return log.StatusCode == val
			}, err
		default:
			return nil, err
		}
	case "resp_header":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return string(log.RespHeader.Encode()) == value
			}, err
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(string(log.RespHeader.Encode()), value)
			}, err
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(string(log.RespHeader.Encode()))
			}, err
		default:
			return nil, err
		}
	case "resp_body":
		switch operation {
		case "term":
			return func(log *AuditLog) bool {
				return log.RespBody == value
			}, nil
		case "match":
			return func(log *AuditLog) bool {
				return strings.Contains(log.RespBody, value)
			}, nil
		case "regexp":
			reg, e := regexp.Compile(value)
			if e != nil {
				return nil, e
			}
			return func(log *AuditLog) bool {
				return reg.MatchString(log.RespBody)
			}, err
		default:
			return nil, err
		}
	case "resp_length":
		val, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return nil, e
		}
		switch operation {
		case "range":
			val2, _ := strconv.ParseInt(value2, 10, 64)
			if value2 == "" {
				return func(log *AuditLog) bool {
					return log.RespLength >= val
				}, err
			} else if value == "" {
				return func(log *AuditLog) bool {
					return log.RespLength <= val2
				}, err
			} else {
				return func(log *AuditLog) bool {
					return log.RespLength >= val && log.RespLength <= val2
				}, err
			}
		case "term":
			return func(log *AuditLog) bool {
				return log.RespLength == val
			}, nil
		default:
			return nil, err
		}
	case "duration":
		val, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return nil, e
		}
		switch operation {
		case "range":
			val2, _ := strconv.ParseInt(value2, 10, 64)
			if value2 == "" {
				return func(log *AuditLog) bool {
					return log.Duration >= val
				}, err
			} else if value == "" {
				return func(log *AuditLog) bool {
					return log.Duration <= val2
				}, err
			} else {
				return func(log *AuditLog) bool {
					return log.Duration >= val && log.Duration <= val2
				}, err
			}
		case "term":
			return func(log *AuditLog) bool {
				return log.Duration == val
			}, nil
		default:
			return nil, err
		}
	}
	return nil, err
}
