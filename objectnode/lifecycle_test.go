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
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrSameRuleID)

	//id = ""
	l1.Rules[0].ID = ""
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMissingRuleID)

	//len(id) > 255
	var id string
	for i := 0; i < 256; i++ {
		id += "a"
	}
	l1.Rules[0].ID = id
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrTooLongRuleID)
	l1.Rules[0].ID = "id"

	//invalid status
	l1.Rules[0].Status = ""
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMalformedXML)
	l1.Rules[0].Status = "Enabled"

	//days < 0
	day := -1
	l1.Rules[0].Expiration.Days = &day
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrDaysType)
	day = 0
	l1.Rules[0].Expiration.Days = &day

	//date
	l1.Rules[0].Expiration.Days = nil
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)
	l1.Rules[0].Expiration.Date = &ti
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrDateType)

	//days and date all nil
	l1.Rules[0].Expiration.Days = nil
	l1.Rules[0].Expiration.Date = nil
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMalformedXML)

	//days and date
	day = 1
	l1.Rules[0].Expiration.Days = &day
	ti = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Expiration.Date = &ti
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMalformedXML)

	l1.Rules[0].Expiration.Date = nil
	day = 1
	l1.Rules[0].Expiration.Days = &day

	l1.Rules[1].Expiration = nil
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMissingActions)

	//no err
	l1.Rules = l1.Rules[:1]
	err = proto.ValidRules(l1.Rules)
	require.NoError(t, err)

	l1.Rules = l1.Rules[:0]
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, err, proto.LifeCycleErrMissingRules)
}

func TestLifecycleConfigurationTransition1(t *testing.T) {
	LifecycleXml := `
<LifecycleConfiguration>
    <Rule>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Transition>
           <Days>365</Days>
           <StorageClass>HDD</StorageClass>
        </Transition>
    </Rule>
</LifecycleConfiguration>
`

	var l1 = NewLifecycleConfiguration()
	err := xml.Unmarshal([]byte(LifecycleXml), l1)
	require.NoError(t, err)

	// test validTransition
	l1.Rules[0].Transitions[0].Days = nil
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	day := 1
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[0].Days = &day
	l1.Rules[0].Transitions[0].Date = &ti
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].Days = nil
	l1.Rules[0].Transitions[0].StorageClass = "SSS"
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].StorageClass = "HDD"
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrDateType, err)

	l1.Rules[0].Transitions[0].Date = nil
	day = 0
	l1.Rules[0].Transitions[0].Days = &day
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrDaysType, err)
}

func TestLifecycleConfigurationTransition2(t *testing.T) {
	LifecycleXml := `
<LifecycleConfiguration>
    <Rule>
        <ID>id1</ID>
        <Status>Enabled</Status>
        <Transition>
           <Days>365</Days>
           <StorageClass>HDD</StorageClass>
        </Transition>
		<Transition>
           <Days>365</Days>
           <StorageClass>HDD</StorageClass>
        </Transition>
    </Rule>
</LifecycleConfiguration>
`

	var l1 = NewLifecycleConfiguration()
	err := xml.Unmarshal([]byte(LifecycleXml), l1)
	require.NoError(t, err)

	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrStorageClass, err)

	//test validTransitions
	l1.Rules[0].Transitions[1].StorageClass = "BLOBSTORE"
	now := time.Now().In(time.UTC)
	ti := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[1].Days = nil
	l1.Rules[0].Transitions[1].Date = &ti
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	ti = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[0].Days = nil
	l1.Rules[0].Transitions[0].Date = &ti
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	t2 := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	l1.Rules[0].Transitions[1].Date = &t2
	l1.Rules[0].Expiration = &proto.Expiration{
		Date: &t2,
	}
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	day1, day2 := 1, 2
	l1.Rules[0].Transitions[0].Days = &day2
	l1.Rules[0].Transitions[0].Date = nil
	l1.Rules[0].Transitions[1].Days = &day1
	l1.Rules[0].Transitions[1].Date = nil
	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	l1.Rules[0].Transitions[0].Days = &day1
	l1.Rules[0].Transitions[1].Days = &day2
	l1.Rules[0].Expiration.Date = nil
	l1.Rules[0].Expiration.Days = &day2

	err = proto.ValidRules(l1.Rules)
	require.Equal(t, proto.LifeCycleErrMalformedXML, err)

	day3 := 3
	l1.Rules[0].Expiration.Days = &day3
	err = proto.ValidRules(l1.Rules)
	require.NoError(t, err)
}
