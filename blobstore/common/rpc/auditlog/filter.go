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
	"math"
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
//			StartTime  string `json:"start_time"`
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

func (filter *Query) Init() error {
	filter.num = 500
	filter.upper = 1000
	all := make([]*FilterFunc, 0)
	for _, clause := range filter.Must {
		for key, valueList := range clause {
			switch key {
			case "term":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("term", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("range", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"must", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
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
						fun, err := parse("term", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("range", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"mustNot", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
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
						fun, err := parse("term", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"should", fun, 0, err}
						or = append(or, &re)
					}
				}
			case "match":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("match", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"should", fun, 0, err}
						or = append(or, &re)
					}
				}
			case "range":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("range", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
						re := FilterFunc{"should", fun, 0, err}
						all = append(all, &re)
					}
				}
			case "regexp":
				for _, value := range valueList.([]interface{}) {
					for field, val := range value.(map[string]interface{}) {
						fun, err := parse("regexp", field, fmt.Sprintf("%v", val))
						if err != nil {
							return err
						}
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
	return nil
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

func parse(operation, field, value string) (func(log *AuditLog) bool, error) {
	var getStrField func(log *AuditLog) string
	var getIntField func(log *AuditLog) int64

	switch field {
	case "req_type", "ReqType":
		getStrField = func(log *AuditLog) string { return log.ReqType }
	case "module", "Module":
		getStrField = func(log *AuditLog) string { return log.Module }
	case "method", "Method":
		getStrField = func(log *AuditLog) string { return log.Method }
	case "path", "Path":
		getStrField = func(log *AuditLog) string { return log.Path }
	case "req_header", "ReqHeader":
		getStrField = func(log *AuditLog) string { return string(log.ReqHeader.Encode()) }
	case "req_params", "ReqParams":
		getStrField = func(log *AuditLog) string { return log.ReqParams }
	case "resp_header", "RespHeader":
		getStrField = func(log *AuditLog) string { return string(log.RespHeader.Encode()) }
	case "resp_body", "RespBody":
		getStrField = func(log *AuditLog) string { return log.RespBody }

	case "start_time", "StartTime":
		getIntField = func(log *AuditLog) int64 { return log.StartTime }
	case "status_code", "StatusCode":
		getIntField = func(log *AuditLog) int64 { return int64(log.StatusCode) }
	case "resp_length", "RespLength":
		getIntField = func(log *AuditLog) int64 { return log.RespLength }
	case "duration", "Duration":
		getIntField = func(log *AuditLog) int64 { return log.Duration }

	default:
		return nil, fmt.Errorf("unsupported field:%s", field)
	}

	if getStrField != nil {
		switch operation {
		case "term":
			return func(log *AuditLog) bool { return getStrField(log) == value }, nil
		case "match":
			return func(log *AuditLog) bool { return strings.Contains(getStrField(log), value) }, nil
		case "regexp":
			reg, err := regexp.Compile(value)
			if err != nil {
				return nil, err
			}
			return func(log *AuditLog) bool { return reg.MatchString(getStrField(log)) }, nil
		default:
			return nil, fmt.Errorf("unsupported operation:%s", operation)
		}
	}
	if getIntField != nil {
		switch operation {
		case "term":
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid value:%s", value)
			}
			return func(log *AuditLog) bool { return getIntField(log) == val }, nil
		case "range":
			strs := strings.Split(value, ",")
			ranges := make([][2]int64, 0, len(strs))
			for _, str := range strs {
				se := strings.Split(str, "-")
				if len(se) != 2 {
					return nil, fmt.Errorf("invalid range:%s", value)
				}

				start := int64(math.MinInt64)
				if se[0] != "" {
					i, err := strconv.ParseInt(se[0], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("invalid range:%s", str)
					}
					start = i
				}
				end := int64(math.MaxInt64)
				if se[1] != "" {
					i, err := strconv.ParseInt(se[1], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("invalid range:%s", str)
					}
					end = i
				}
				ranges = append(ranges, [2]int64{start, end})
			}

			return func(log *AuditLog) bool {
				val := getIntField(log)
				for idx := range ranges {
					if val >= ranges[idx][0] && val <= ranges[idx][1] {
						return true
					}
				}
				return false
			}, nil
		default:
			return nil, fmt.Errorf("unsupported operation:%s", operation)
		}
	}

	return nil, fmt.Errorf("unsupported field:%s operation:%s", field, operation)
}
