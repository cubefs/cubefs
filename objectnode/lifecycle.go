// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"net/http"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	RuleMaxCounts = 1000
	MaxIdLength   = 255
)

var (
	LifeCycleErrTooManyRules     = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Rules Number should not exceed allowed limit of 1000.", StatusCode: http.StatusBadRequest}
	LifeCycleErrMissingRules     = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "No Lifecycle Rules found in request.", StatusCode: http.StatusBadRequest}
	LifeCycleErrMissingActions   = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "At least one action needs to be specified in a rule.", StatusCode: http.StatusBadRequest}
	LifeCycleErrMissingRuleID    = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "No Lifecycle Rule ID in request.", StatusCode: http.StatusBadRequest}
	LifeCycleErrTooLongRuleID    = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "ID length should not exceed allowed limit of 255.", StatusCode: http.StatusBadRequest}
	LifeCycleErrSameRuleID       = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Rule ID must be unique. Found same ID for more than one rule.", StatusCode: http.StatusBadRequest}
	LifeCycleErrDateType         = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "'Date' must be at midnight GMT.", StatusCode: http.StatusBadRequest}
	LifeCycleErrDaysType         = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "'Days' for Expiration action must be a positive integer.", StatusCode: http.StatusBadRequest}
	LifeCycleErrStorageClass     = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "'StorageClass' must be different for 'Transition' actions in same 'Rule'", StatusCode: http.StatusBadRequest}
	LifeCycleErrMalformedXML     = &ErrorCode{ErrorCode: "MalformedXML", ErrorMessage: "The XML you provided was not well-formed or did not validate against our published schema.", StatusCode: http.StatusBadRequest}
	NoSuchLifecycleConfiguration = &ErrorCode{ErrorCode: "NoSuchLifecycleConfiguration", ErrorMessage: "The lifecycle configuration does not exist.", StatusCode: http.StatusNotFound}
)

type LifecycleConfiguration struct {
	Rules []*proto.Rule `json:"Rule,omitempty" xml:"Rule,omitempty" bson:"Rule,omitempty"`
}

func NewLifecycleConfiguration() *LifecycleConfiguration {
	return &LifecycleConfiguration{}
}

func (l *LifecycleConfiguration) Validate() (bool, *ErrorCode) {
	if len(l.Rules) > RuleMaxCounts {
		return false, LifeCycleErrTooManyRules
	}
	if len(l.Rules) <= 0 {
		return false, LifeCycleErrMissingRules
	}

	isRuleIdExist := make(map[string]bool)
	for _, rule := range l.Rules {
		_, ok := isRuleIdExist[rule.ID]
		if !ok {
			isRuleIdExist[rule.ID] = true
		} else {
			return false, LifeCycleErrSameRuleID
		}
		if err := validRule(rule); err != nil {
			return false, err
		}
	}

	return true, nil
}

func validRule(r *proto.Rule) *ErrorCode {
	if len(r.ID) == 0 {
		return LifeCycleErrMissingRuleID
	}
	if len(r.ID) > MaxIdLength {
		return LifeCycleErrTooLongRuleID
	}
	if r.Status != proto.RuleEnabled && r.Status != proto.RuleDisabled {
		return LifeCycleErrMalformedXML
	}

	if r.Expiration == nil && r.Transitions == nil {
		return LifeCycleErrMissingActions
	}

	if r.Expiration != nil {
		if err := validExpiration(r.Expiration); err != nil {
			return err
		}
	}

	if r.Transitions != nil {
		daysMap := make(map[string]int)
		dateMap := make(map[string]*time.Time)
		singleMap := make(map[string]int)
		for _, transition := range r.Transitions {
			singleMap[transition.StorageClass]++
			if singleMap[transition.StorageClass] > 1 {
				return LifeCycleErrStorageClass
			}
			if err := validTransition(transition, dateMap, daysMap); err != nil {
				return err
			}
		}

		if err := validTransitions(dateMap, daysMap, r.Expiration); err != nil {
			return err
		}
	}

	return nil
}

func validExpiration(e *proto.Expiration) *ErrorCode {
	// Date and Days cannot be set at the same time
	if e.Date != nil && e.Days != nil {
		return LifeCycleErrMalformedXML
	}
	// Date and Days cannot both be nil
	if e.Date == nil && e.Days == nil {
		return LifeCycleErrMalformedXML
	}
	// Date must be midnight UTC
	if e.Date != nil {
		date := e.Date.In(time.UTC)
		if !(date.Hour() == 0 && date.Minute() == 0 && date.Second() == 0 && date.Nanosecond() == 0) {
			return LifeCycleErrDateType
		}
	} else if e.Days != nil {
		// Days must be greater than 0
		if *e.Days <= 0 {
			return LifeCycleErrDaysType
		}
	}

	return nil
}

var transitionStorageClass = map[string]bool{
	proto.OpTypeStorageClassHDD: true,
	proto.OpTypeStorageClassEBS: true,
}

func validTransition(t *proto.Transition, dateMap map[string]*time.Time, daysMap map[string]int) *ErrorCode {
	// Date and Days cannot be set at the same time
	if t.Date != nil && t.Days != nil {
		return LifeCycleErrMalformedXML
	}
	// Date and Days cannot both be nil
	if t.Date == nil && t.Days == nil {
		return LifeCycleErrMalformedXML
	}
	// StorageClass must be the specified
	if !transitionStorageClass[t.StorageClass] {
		return LifeCycleErrMalformedXML
	}
	// Date must be midnight UTC
	if t.Date != nil {
		date := t.Date.In(time.UTC)
		if !(date.Hour() == 0 && date.Minute() == 0 && date.Second() == 0 && date.Nanosecond() == 0) {
			return LifeCycleErrDateType
		}
		dateMap[t.StorageClass] = t.Date
	} else if t.Days != nil {
		// Days must be greater than 0
		if *t.Days <= 0 {
			return LifeCycleErrDaysType
		}
		daysMap[t.StorageClass] = *t.Days
	}

	return nil
}

func validTransitions(dateMap map[string]*time.Time, daysMap map[string]int, expiration *proto.Expiration) *ErrorCode {
	// transitions and expiration must be all in date form or all in days form
	if len(dateMap) > 0 && len(daysMap) > 0 {
		return LifeCycleErrMalformedXML
	}

	if len(dateMap) > 0 {
		var s []*time.Time
		if c, ok := dateMap[proto.OpTypeStorageClassHDD]; ok {
			s = append(s, c)
		}
		if c, ok := dateMap[proto.OpTypeStorageClassEBS]; ok {
			s = append(s, c)
		}
		for i := 0; i < len(s)-1; i++ {
			if !s[i+1].After(*s[i]) {
				return LifeCycleErrMalformedXML
			}
		}
		if expiration != nil {
			if expiration.Days != nil || !expiration.Date.After(*s[len(s)-1]) {
				return LifeCycleErrMalformedXML
			}
		}
	}

	if len(daysMap) > 0 {
		var s []int
		if c, ok := daysMap[proto.OpTypeStorageClassHDD]; ok {
			s = append(s, c)
		}
		if c, ok := daysMap[proto.OpTypeStorageClassEBS]; ok {
			s = append(s, c)
		}
		for i := 0; i < len(s)-1; i++ {
			if s[i+1] <= s[i] {
				return LifeCycleErrMalformedXML
			}
		}
		if expiration != nil {
			if expiration.Date != nil || *expiration.Days <= s[len(s)-1] {
				return LifeCycleErrMalformedXML
			}
		}
	}

	return nil
}
