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
	"encoding/xml"
	"net/http"
	"time"
)

const (
	ExpirationStatusEnabled  = "Enabled"
	ExpirationStatusDisabled = "Disabled"
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
	LifeCycleErrMalformedXML     = &ErrorCode{ErrorCode: "MalformedXML", ErrorMessage: "The XML you provided was not well-formed or did not validate against our published schema.", StatusCode: http.StatusBadRequest}
	NoSuchLifecycleConfiguration = &ErrorCode{ErrorCode: "NoSuchLifecycleConfiguration", ErrorMessage: "The lifecycle configuration does not exist.", StatusCode: http.StatusNotFound}
)

type LifeCycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Rules   []*Rule  `xml:"Rule,omitempty"`
}

type Rule struct {
	XMLName xml.Name    `xml:"Rule"`
	Expire  *Expiration `xml:"Expiration"`
	Filter  *Filter     `xml:"Filter"`
	ID      string      `xml:"ID"`
	Status  string      `xml:"Status"`
}

type Expiration struct {
	XMLName xml.Name   `xml:"Expiration"`
	Date    *time.Time `xml:"Date,omitempty"`
	Days    *int       `xml:"Days,omitempty"`
}

type Filter struct {
	XMLName xml.Name `xml:"Filter"`
	Prefix  string   `xml:"Prefix,omitempty"`
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		XMLName: xml.Name{Local: "LifecycleConfiguration"},
	}
}

func (l *LifeCycle) Validate() (bool, *ErrorCode) {
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
		if err := rule.valid(); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (r *Rule) valid() *ErrorCode {
	if len(r.ID) == 0 {
		return LifeCycleErrMissingRuleID
	}
	if len(r.ID) > MaxIdLength {
		return LifeCycleErrTooLongRuleID
	}
	if r.Status != ExpirationStatusEnabled && r.Status != ExpirationStatusDisabled {
		return LifeCycleErrMalformedXML
	}

	if r.Expire == nil {
		return LifeCycleErrMissingActions
	}

	if err := r.Expire.validExpiration(); err != nil {
		return err
	}

	return nil
}

func (e *Expiration) validExpiration() *ErrorCode {
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
