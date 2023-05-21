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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseObjectLockConfig(t *testing.T) {
	tests := []struct {
		value       string
		expectedErr error
	}{
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled></ObjectLockEnabled>
					</ObjectLockConfiguration>`,
			expectedErr: nil,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/" >
						<ObjectLockEnabled>enable</ObjectLockEnabled>
					</ObjectLockConfiguration>`,
			expectedErr: InvalidObjectLockEnabledErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
					</ObjectLockConfiguration>`,
			expectedErr: NilDefaultRuleErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule></Rule>
					</ObjectLockConfiguration>`,
			expectedErr: NilDefaultRetentionErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>compliance</Mode>
								<Days>30</Days>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: InvalidModeErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: EitherDaysOrYearsSpecifiedErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Days>10</Days>
								<Years>10</Years>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: BothDaysAndYearsSpecifiedErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Days>0</Days>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: NonPositiveRetentionPeriodsErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Years>0</Years>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: NonPositiveRetentionPeriodsErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Days>300000</Days>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: RetentionPeriodsTooLargeErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Years>100</Years>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: RetentionPeriodsTooLargeErr,
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Years>10</Years>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		_, err := ParseObjectLockConfigFromXML([]byte(tt.value))
		if err != nil {
			require.Equal(t, tt.expectedErr.Error(), err.Error())
		}
	}
}

func TestToRetention(t *testing.T) {
	tests := []struct {
		value               string
		expectedToRetention *Retention
	}{
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Years>10</Years>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedToRetention: &Retention{
				Mode:     "COMPLIANCE",
				Duration: 10 * 365 * nanosecondsPerDay,
			},
		},
		{
			value: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<ObjectLockEnabled>Enabled</ObjectLockEnabled>
						<Rule>
							<DefaultRetention>
								<Mode>COMPLIANCE</Mode>
								<Days>365</Days>
							</DefaultRetention>
						</Rule>
					</ObjectLockConfiguration>`,
			expectedToRetention: &Retention{
				Mode:     "COMPLIANCE",
				Duration: 365 * nanosecondsPerDay,
			},
		},
	}
	for _, tt := range tests {
		o, err := ParseObjectLockConfigFromXML([]byte(tt.value))
		require.NoError(t, err)
		require.Equal(t, o.ToRetention(), tt.expectedToRetention)
		now := time.Date(2023, 5, 26, 0, 0, 0, 0, time.UTC)
		timeUnixNano := now.Add(tt.expectedToRetention.Duration).UnixNano()
		timeUnixNanoStr := strconv.FormatInt(timeUnixNano, 10)
		retentionDateStr := formatRetentionDateStr(now, o.ToRetention())
		require.Equal(t, timeUnixNanoStr, retentionDateStr)
	}
}

func TestMarshalXML(t *testing.T) {
	retentionDate := RetentionDate{
		Time: time.Date(2023, 5, 26, 0, 0, 0, 0, time.UTC),
	}
	objectRetention := ObjectRetention{
		Mode:            ComplianceMode,
		RetainUntilDate: retentionDate,
	}
	_, err := xml.Marshal(objectRetention)
	require.NoError(t, err)
}
