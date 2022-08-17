// Copyright 2019 The ChubaoFS Authors.
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
	"github.com/cubefs/cubefs/util/log"
	"time"
)

type ExpirationStatus string

const (
	ExpirationStatusEnabled  ExpirationStatus = "Enabled"
	ExpirationStatusDisabled ExpirationStatus = "Disabled"
)

const (
	RuleMaxCounts = 1000
)

type LifeCycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Rules   []*Rule  `xml:"Rule,omitempty"`
}

type Rule struct {
	XMLName              xml.Name                        `xml:"Rule"`
	AbortMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	Expire               *Expiration                     `xml:"Expiration"`
	Filter               *FilterConfig                   `xml:"Filter"`
	ID                   string                          `xml:"ID"`
	Prefix               string                          `xml:"Prefix,omitempty"`
	Status               ExpirationStatus                `xml:"Status"`
}

type AbortIncompleteMultipartUpload struct {
	XMLName             xml.Name `xml:"AbortIncompleteMultipartUpload"`
	DaysAfterInitiation int      `xml:"DaysAfterInitiation"`
}

type Expiration struct {
	XMLName xml.Name   `xml:"Expiration"`
	Date    *time.Time `xml:"Date,omitempty"`
	Days    int        `xml:"Days,omitempty"`
}

type FilterConfig struct {
	XMLName               xml.Name   `xml:"Filter"`
	And                   *AndOpr    `xml:"And,omitempty"`
	ObjectSizeGreaterThan int64      `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    int64      `xml:"ObjectSizeLessThan,omitempty"`
	Prefix                string     `xml:"Prefix,omitempty"`
	Tag                   *TagConfig `xml:"Tag,omitempty"`
}

type AndOpr struct {
	XMLName               xml.Name     `xml:"And"`
	ObjectSizeGreaterThan int64        `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    int64        `xml:"ObjectSizeLessThan,omitempty"`
	Prefix                string       `xml:"Prefix,omitempty"`
	Tag                   []*TagConfig `xml:"Tag,omitempty"`
}

type TagConfig struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		XMLName: xml.Name{Local: "LifecycleConfiguration"},
	}
}

func (l *LifeCycle) Validate() (bool, *ErrorCode) {
	var errorCode *ErrorCode
	if len(l.Rules) > RuleMaxCounts {
		return false, LifeCycleRulesGreaterThen1K
	}
	if len(l.Rules) <= 0 {
		return false, LifeCycleRulesLessThenOne
	}

	ruleIds := make(map[string]int)
	for _, rule := range l.Rules {
		if _, ok := ruleIds[rule.ID]; ok {
			return false, LifeCycleRulesInvalid
		} else {
			ruleIds[rule.ID] = 1
		}
		log.LogDebugf("Validate: rule : (%v)", rule)
		if rule.Expire == nil {
			return false, LifeCycleAtLeastOneAction
		}

	}
	return true, errorCode
}
