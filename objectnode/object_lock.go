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
	"errors"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

var (
	InvalidModeErr                 = errors.New("Invalid Retention Mode")
	RetentionPeriodsTooLargeErr    = errors.New("Default retention period is too large")
	NonPositiveRetentionPeriodsErr = errors.New("Default retention period must be a positive integer value")
	BothDaysAndYearsSpecifiedErr   = errors.New("Either Days or Years must be specified, cannot be specified both at the same time")
	EitherDaysOrYearsSpecifiedErr  = errors.New("Either Days or Years must be specified")
	InvalidObjectLockEnabledErr    = errors.New("Only Enabled value is allowd to ObjectLockEnabled element")
	NilDefaultRetentionErr         = errors.New("Default retention cannot be nil")
	NilDefaultRuleErr              = errors.New("Default rule cannot be nil")
)

const (
	ComplianceMode = "COMPLIANCE"
	Enabled        = "Enabled"

	MaxObjectLockSize     = 1 << 12 // 16KB
	maximumRetentionDays  = 70 * 365
	maximumRetentionYears = 70
	nanosecondsPerDay     = 24 * 60 * 60 * 1e9
)

type ObjectLockConfig struct {
	XMLNS             string          `xml:"xmlns,attr,omitempty" json:"-" `
	XMLName           *xml.Name       `xml:"ObjectLockConfiguration" json:"-" `
	ObjectLockEnabled string          `xml:"ObjectLockEnabled" json:"object_lock_enabled,omitempty" `
	Rule              *ObjectLockRule `xml:"Rule,omitempty" json:"rule,omitempty"`
}

type ObjectLockRule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention" json:"default_retention" `
}

type DefaultRetention struct {
	XMLName *xml.Name `xml:"DefaultRetention" json:"-"`
	Mode    string    `xml:"Mode" json:"mode"`
	Days    *int64    `xml:"Days,omitempty" json:"days,omitempty" `
	Years   *int64    `xml:"Years,omitempty" json:"years,omitempty"`
}

func (c *ObjectLockConfig) CheckValid() error {
	if c.IsEmpty() {
		return nil
	}

	if c.ObjectLockEnabled != Enabled {
		return InvalidObjectLockEnabledErr
	}

	if c.Rule == nil {
		return NilDefaultRuleErr
	}

	if c.Rule != nil && c.Rule.DefaultRetention == nil {
		return NilDefaultRetentionErr
	}

	if err := c.Rule.DefaultRetention.isValid(); err != nil {
		return err
	}
	return nil
}

type Retention struct {
	Mode     string
	Duration time.Duration
}

func (c ObjectLockConfig) ToRetention() (r *Retention) {
	if c.Rule != nil {
		r = &Retention{}
		r.Mode = c.Rule.DefaultRetention.Mode

		if c.Rule.DefaultRetention.Days != nil {
			r.Duration = time.Duration(nanosecondsPerDay * (*c.Rule.DefaultRetention.Days))
		} else {
			r.Duration = time.Duration(nanosecondsPerDay*(*c.Rule.DefaultRetention.Years)) * 365
		}
	}
	return
}

// check whether ObjectLockConfig is empty
func (c ObjectLockConfig) IsEmpty() bool {
	return c.ObjectLockEnabled == "" && c.Rule == nil
}

// parse ObjectLockConfig from xml
func ParseObjectLockConfigFromXML(data []byte) (*ObjectLockConfig, error) {
	config := ObjectLockConfig{}
	if err := xml.Unmarshal(data, &config); err != nil {
		return nil, NewError("InvalidObjectLockConfiguration", err.Error(), 400)
	}
	err := config.CheckValid()
	if err != nil {
		return nil, NewError("InvalidObjectLockConfiguration", err.Error(), 400)
	}
	return &config, nil
}

// check valid of DefaultRetention
func (d DefaultRetention) isValid() error {
	switch d.Mode {
	case ComplianceMode:
	default:
		return InvalidModeErr
	}
	if d.Days == nil && d.Years == nil {
		return EitherDaysOrYearsSpecifiedErr
	}

	if d.Days != nil && d.Years != nil {
		return BothDaysAndYearsSpecifiedErr
	}

	if d.Days != nil {
		if *d.Days <= 0 {
			return NonPositiveRetentionPeriodsErr
		}
		if *d.Days > maximumRetentionDays {
			return RetentionPeriodsTooLargeErr
		}
	}
	if d.Years != nil {
		if *d.Years <= 0 {
			return NonPositiveRetentionPeriodsErr
		}
		if *d.Years > maximumRetentionYears {
			return RetentionPeriodsTooLargeErr
		}
	}

	return nil
}

type ObjectRetention struct {
	XMLNS           string        `xml:"xmlns,attr,omitempty"`
	XMLName         xml.Name      `xml:"Retention"`
	Mode            string        `xml:"Mode,omitempty"`
	RetainUntilDate RetentionDate `xml:"RetainUntilDate,omitempty"`
}

type RetentionDate struct {
	time.Time
}

func (r RetentionDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if r == (RetentionDate{time.Time{}}) {
		return nil
	}
	return e.EncodeElement(r.Format(ISO8601Layout), startElement)
}

func storeObjectLock(bytes []byte, vol *Volume) (err error) {
	return vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSLock, bytes)
}

func isObjectLocked(v *Volume, inode uint64, name, path string) error {
	xattrInfo, err := v.mw.XAttrGet_ll(inode, XAttrKeyOSSLock)
	if err != nil {
		log.LogErrorf("isObjectLocked: check ObjectLock err(%v) volume(%v) path(%v) name(%v)",
			err, v.name, path, name)
		return err
	}
	retainUntilDate := xattrInfo.Get(XAttrKeyOSSLock)
	if len(retainUntilDate) > 0 {
		retainUntilDateInt64, err := strconv.ParseInt(string(retainUntilDate), 10, 64)
		if err != nil {
			return err
		}
		if retainUntilDateInt64 > time.Now().UnixNano() {
			log.LogWarnf("isObjectLocked: object is locked, retainUntilDate(%v) volume(%v) path(%v) name(%v)",
				retainUntilDateInt64, v.name, path, name)
			return AccessDenied
		}
	}
	return nil
}

func formatRetentionDateStr(modifyTime time.Time, retention *Retention) string {
	retentionDateUnixNano := modifyTime.Add(retention.Duration).UnixNano()
	return strconv.FormatInt(retentionDateUnixNano, 10)
}
