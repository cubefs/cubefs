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
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestLifecycleConfiguration(t *testing.T) {
	LifecycleXml := `
<LifecycleConfiguration>
    <Rule>
        <Filter>
           <Prefix>logs/</Prefix>
        </Filter>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Expiration>
           <Days>365</Days>
        </Expiration>
    </Rule>
    <Rule>
        <Filter>
           <Prefix>logs/</Prefix>
        </Filter>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Expiration>
           <Days>365</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>
`

	var l1 = NewLifecycleConfiguration()
	err := xml.Unmarshal([]byte(LifecycleXml), l1)
	require.NoError(t, err)

	//same id
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrSameRuleID)

	//id = ""
	l1.Rules[0].ID = ""
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMissingRuleID)

	//len(id) > 255
	var id string
	for i := 0; i < 256; i++ {
		id += "a"
	}
	l1.Rules[0].ID = id
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrTooLongRuleID)
	l1.Rules[0].ID = "id"

	//invalid status
	l1.Rules[0].Status = ""
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMalformedXML)
	l1.Rules[0].Status = "Enabled"

	//days < 0
	day := -1
	l1.Rules[0].Expiration.Days = &day
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrDaysType)
	day = 0
	l1.Rules[0].Expiration.Days = &day

	//date
	l1.Rules[0].Expiration.Days = nil
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)
	l1.Rules[0].Expiration.Date = &ti
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrDateType)

	//days and date all nil
	l1.Rules[0].Expiration.Days = nil
	l1.Rules[0].Expiration.Date = nil
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMalformedXML)

	//days and date
	day = 1
	l1.Rules[0].Expiration.Days = &day
	ti = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Expiration.Date = &ti
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMalformedXML)

	l1.Rules[0].Expiration.Date = nil
	day = 1
	l1.Rules[0].Expiration.Days = &day

	l1.Rules[1].Expiration = nil
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMissingActions)

	//no err
	l1.Rules = l1.Rules[:1]
	ok, _ := l1.Validate()
	require.Equal(t, true, ok)

	l1.Rules = l1.Rules[:0]
	_, err = l1.Validate()
	require.Equal(t, err, LifeCycleErrMissingRules)
}

func TestLifecycleConfigurationTransition1(t *testing.T) {
	LifecycleXml := `
<LifecycleConfiguration>
    <Rule>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Transition>
           <Days>365</Days>
           <StorageClass>STANDARD_IA</StorageClass>
        </Transition>
    </Rule>
</LifecycleConfiguration>
`

	var l1 = NewLifecycleConfiguration()
	err := xml.Unmarshal([]byte(LifecycleXml), l1)
	require.NoError(t, err)

	// test validTransition
	l1.Rules[0].Transitions[0].Days = nil
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	day := 1
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[0].Days = &day
	l1.Rules[0].Transitions[0].Date = &ti
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].Days = nil
	l1.Rules[0].Transitions[0].StorageClass = "SSS"
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].StorageClass = "STANDARD_IA"
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrDateType, err)

	l1.Rules[0].Transitions[0].Date = nil
	day = 0
	l1.Rules[0].Transitions[0].Days = &day
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrDaysType, err)
}

func TestLifecycleConfigurationTransition2(t *testing.T) {
	LifecycleXml := `
<LifecycleConfiguration>
    <Rule>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Transition>
           <Days>365</Days>
           <StorageClass>STANDARD_IA</StorageClass>
        </Transition>
		<Transition>
           <Days>365</Days>
           <StorageClass>STANDARD_IA</StorageClass>
        </Transition>
    </Rule>
</LifecycleConfiguration>
`

	var l1 = NewLifecycleConfiguration()
	err := xml.Unmarshal([]byte(LifecycleXml), l1)
	require.NoError(t, err)

	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrStorageClass, err)

	//test validTransitions
	l1.Rules[0].Transitions[1].StorageClass = "GLACIER"
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[1].Days = nil
	l1.Rules[0].Transitions[1].Date = &ti
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	ti = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[0].Days = nil
	l1.Rules[0].Transitions[0].Date = &ti
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	t2 := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[1].Date = &t2
	l1.Rules[0].Expiration = &proto.Expiration{
		Date: &t2,
	}
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	day1, day2 := 1, 2
	l1.Rules[0].Transitions[0].Days = &day2
	l1.Rules[0].Transitions[0].Date = nil
	l1.Rules[0].Transitions[1].Days = &day1
	l1.Rules[0].Transitions[1].Date = nil
	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].Days = &day1
	l1.Rules[0].Transitions[1].Days = &day2
	l1.Rules[0].Expiration.Date = nil
	l1.Rules[0].Expiration.Days = &day2

	_, err = l1.Validate()
	require.Equal(t, LifeCycleErrMalformedXML, err)

	day3 := 3
	l1.Rules[0].Expiration.Days = &day3
	ok, _ := l1.Validate()
	require.Equal(t, true, ok)
}
