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
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

const (
	NotificationARNPrefix = "arn:cfs:"
)

const (
	BucketRemoved                          EventName = "s3:BucketRemoved:*"
	ObjectCreatedAll                       EventName = "s3:ObjectCreated:*"
	ObjectCreatedPut                       EventName = "s3:ObjectCreated:Put"
	ObjectCreatedPost                      EventName = "s3:ObjectCreated:Post"
	ObjectCreatedCopy                      EventName = "s3:ObjectCreated:Copy"
	ObjectCreatedCompleteMultipartUpload   EventName = "s3:ObjectCreated:CompleteMultipartUpload"
	ObjectRemovedAll                       EventName = "s3:ObjectRemoved:*"
	ObjectRemovedDelete                    EventName = "s3:ObjectRemoved:Delete"
	ObjectRemovedDeleteMarkerCreated       EventName = "s3:ObjectRemoved:DeleteMarkerCreated"
	ObjectRestoreAll                       EventName = "s3:ObjectRestore:*"
	ObjectRestorePost                      EventName = "s3:ObjectRestore:Post"
	ObjectRestoreCompleted                 EventName = "s3:ObjectRestore:Completed"
	ObjectRestoreDelete                    EventName = "s3:ObjectRestore:Delete"
	ObjectAccessedAll                      EventName = "s3:ObjectAccessed:*"
	ObjectAccessedGet                      EventName = "s3:ObjectAccessed:Get"
	ObjectAccessedHead                     EventName = "s3:ObjectAccessed:Head"
	ObjectAccessedGetRetention             EventName = "s3:ObjectAccessed:GetRetention"
	ObjectTaggingAll                       EventName = "s3:ObjectTagging:*"
	ObjectTaggingPut                       EventName = "s3:ObjectTagging:Put"
	ObjectTaggingDelete                    EventName = "s3:ObjectTagging:Delete"
	ObjectAclPut                           EventName = "s3:ObjectAcl:Put"
	ObjectLockPut                          EventName = "s3:ObjectLock:Put"
	LifecyclePut                           EventName = "s3:Lifecycle:Put"
	LifecycleExpirationAll                 EventName = "s3:LifecycleExpiration:*"
	LifecycleExpirationDelete              EventName = "s3:LifecycleExpiration:Delete"
	LifecycleExpirationDeleteMarkerCreated EventName = "s3:LifecycleExpiration:DeleteMarkerCreated"
)

var (
	NotificationService = []string{"sqs", "sns", "lambda"}
)

type NotifierID struct {
	ID   string
	Name string
}

func (n NotifierID) String() string {
	return n.ID + ":" + n.Name
}

type NotifierIDSet map[NotifierID]struct{}

func (n NotifierIDSet) Copy() NotifierIDSet {
	ns := make(NotifierIDSet)
	for k, v := range n {
		ns[k] = v
	}

	return ns
}

func (n NotifierIDSet) Add(ns NotifierIDSet) NotifierIDSet {
	nsc := n.Copy()
	for k := range ns {
		nsc[k] = struct{}{}
	}

	return nsc
}

type FilterKeyMap map[FilterKey]NotifierIDSet

func (m FilterKeyMap) Copy() FilterKeyMap {
	fm := make(FilterKeyMap)
	for k, ns := range m {
		fm[k] = ns.Copy()
	}

	return fm
}

func (m FilterKeyMap) Add(fm FilterKeyMap) FilterKeyMap {
	fmc := m.Copy()
	for k, ns := range fm {
		fmc[k] = fmc[k].Add(ns)
	}

	return fmc
}

func (m FilterKeyMap) Match(key string) NotifierIDSet {
	ns := NotifierIDSet{}
	for fk, nns := range m {
		if fk.Match(key) {
			ns = ns.Add(nns)
		}
	}

	return ns
}

type EventFilter map[EventName]FilterKeyMap

func (e EventFilter) Add(ef EventFilter) {
	for en, fm := range ef {
		e[en] = fm.Add(e[en])
	}
}

func (e EventFilter) Match(name EventName, key string) NotifierIDSet {
	return e[name].Match(key)
}

type EventName string

func (e EventName) Validate() error {
	switch e {
	case BucketRemoved,
		ObjectCreatedAll,
		ObjectCreatedPut,
		ObjectCreatedPost,
		ObjectCreatedCopy,
		ObjectCreatedCompleteMultipartUpload,
		ObjectAccessedAll,
		ObjectAccessedGet,
		ObjectAccessedHead,
		ObjectAccessedGetRetention,
		ObjectRemovedAll,
		ObjectRemovedDelete,
		ObjectRemovedDeleteMarkerCreated,
		ObjectRestoreAll,
		ObjectRestorePost,
		ObjectRestoreCompleted,
		ObjectRestoreDelete,
		ObjectTaggingAll,
		ObjectTaggingPut,
		ObjectTaggingDelete,
		ObjectAclPut,
		ObjectLockPut,
		LifecyclePut,
		LifecycleExpirationAll,
		LifecycleExpirationDelete,
		LifecycleExpirationDeleteMarkerCreated:
		return nil
	default:
		return fmt.Errorf("unsupported event '%s'", e)
	}
}

func (e EventName) Expand() []EventName {
	switch e {
	case ObjectCreatedAll:
		return []EventName{
			ObjectCreatedPut,
			ObjectCreatedPost,
			ObjectCreatedCopy,
			ObjectCreatedCompleteMultipartUpload,
		}
	case ObjectAccessedAll:
		return []EventName{
			ObjectAccessedGet,
			ObjectAccessedHead,
			ObjectAccessedGetRetention,
		}
	case ObjectRemovedAll:
		return []EventName{
			ObjectRemovedDelete,
			ObjectRemovedDeleteMarkerCreated,
		}
	case ObjectRestoreAll:
		return []EventName{
			ObjectRestorePost,
			ObjectRestoreCompleted,
			ObjectRestoreDelete,
		}
	case ObjectTaggingAll:
		return []EventName{
			ObjectTaggingPut,
			ObjectTaggingDelete,
		}
	case LifecycleExpirationAll:
		return []EventName{
			LifecycleExpirationDelete,
			LifecycleExpirationDeleteMarkerCreated,
		}
	default:
		return []EventName{e}
	}
}

type FilterKey struct {
	Prefix string
	Suffix string
}

func (k *FilterKey) Match(key string) bool {
	if k.Prefix != "" && !strings.HasPrefix(key, k.Prefix) {
		return false
	}

	if k.Suffix != "" && !strings.HasSuffix(key, k.Suffix) {
		return false
	}

	return true
}

type FilterRule struct {
	Name  string `xml:"Name" json:"name"`
	Value string `xml:"Value" json:"value"`
}

type S3Key struct {
	FilterRules []FilterRule `xml:"FilterRule,omitempty" json:"rules,omitempty"`
}

type NotificationCommon struct {
	ID     string      `xml:"Id,omitempty" json:"id,omitempty"`
	Filter S3Key       `xml:"Filter>S3Key,omitempty" json:"filter,omitempty"`
	Events []EventName `xml:"Event" json:"event"`
}

func (nc *NotificationCommon) Validate() error {
	if len(nc.Events) == 0 {
		return errors.New("missing event configuration")
	}

	events := make(map[EventName]struct{})
	for _, en := range nc.Events {
		if err := en.Validate(); err != nil {
			return err
		}
		if _, ok := events[en]; ok {
			return fmt.Errorf("duplicate event '%s'", en)
		}
		events[en] = struct{}{}
	}

	if len(nc.Filter.FilterRules) > 0 {
		rules := make(map[string]struct{})
		for _, rule := range nc.Filter.FilterRules {
			switch rule.Name {
			case "prefix", "suffix":
				if _, ok := rules[rule.Name]; ok {
					return fmt.Errorf("duplicate filter rule name '%s'", rule.Name)
				}
				rules[rule.Name] = struct{}{}
			default:
				return fmt.Errorf("invalid filter rule name '%s'", rule.Name)
			}
		}
	}

	return nil
}

func (nc *NotificationCommon) FilterKey() FilterKey {
	var fk FilterKey
	for _, rule := range nc.Filter.FilterRules {
		switch rule.Name {
		case "prefix":
			fk.Prefix = rule.Value
		case "suffix":
			fk.Suffix = rule.Value
		}
	}

	return fk
}

type NotificationARN struct {
	Service string `json:"svc"`
	Region  string `json:"region"`
	ID      string `json:"id"`
	Name    string `json:"name"`
}

func (na NotificationARN) String() string {
	return NotificationARNPrefix + na.Service + ":" + na.Region + ":" + na.ID + ":" + na.Name
}

func (na *NotificationARN) NotifierID() NotifierID {
	return NotifierID{ID: na.ID, Name: na.Name}
}

func (na *NotificationARN) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return e.EncodeElement(na.String(), start)
}

func (na *NotificationARN) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var arn string
	if err := d.DecodeElement(&arn, &start); err != nil {
		return err
	}

	if !strings.HasPrefix(arn, NotificationARNPrefix) {
		return fmt.Errorf("invalid ARN '%s'", arn)
	}

	parts := strings.Split(arn, ":")
	if len(parts) != 6 || !IsNotificationService(parts[2]) || parts[4] == "" || parts[5] == "" {
		return fmt.Errorf("invalid ARN '%s'", arn)
	}

	*na = NotificationARN{
		Service: parts[2],
		Region:  parts[3],
		ID:      parts[4],
		Name:    parts[5],
	}

	return nil
}

func (na *NotificationARN) Validate(region string, mgr *NotificationMgr) error {
	if mgr == nil {
		return errors.New("service does not support event notifications")
	}

	if na.Region != "" && na.Region != region {
		return fmt.Errorf("invalid region '%s' in ARN", na.Region)
	}

	if mgr.GetNotifier(na.NotifierID()) == nil {
		return fmt.Errorf("ARN '%s' not found", na)
	}

	return nil
}

type NotificationLambda struct {
	// NotificationCommon
	ARN NotificationARN `xml:"CloudFunction" json:"arn"`
}

type NotificationTopic struct {
	// NotificationCommon
	ARN NotificationARN `xml:"Topic" json:"arn"`
}

type NotificationQueue struct {
	NotificationCommon
	ARN NotificationARN `xml:"Queue" json:"arn"`
}

func (nq *NotificationQueue) Validate(region string, mgr *NotificationMgr) error {
	if err := nq.ARN.Validate(region, mgr); err != nil {
		return err
	}

	return nq.NotificationCommon.Validate()
}

type NotificationConfig struct {
	XMLNS   string               `xml:"xmlns,attr,omitempty" json:"-"`
	XMLName xml.Name             `xml:"NotificationConfiguration" json:"-"`
	Lambdas []NotificationLambda `xml:"CloudFunctionConfiguration,omitempty" json:"lambda,omitempty"`
	Queues  []NotificationQueue  `xml:"QueueConfiguration,omitempty" json:"queue,omitempty"`
	Topics  []NotificationTopic  `xml:"TopicConfiguration,omitempty" json:"topic,omitempty"`
}

func (c *NotificationConfig) Validate(region string, mgr *NotificationMgr) error {
	if len(c.Lambdas) > 0 || len(c.Topics) > 0 {
		return errors.New("could func or topic configuration is not supported")
	}

	if len(c.Queues) > 0 {
		for i, q1 := range c.Queues[:len(c.Queues)-1] {
			if err := q1.Validate(region, mgr); err != nil {
				return err
			}
			q1.ID = ""
			for _, q2 := range c.Queues[i+1:] {
				q2.ID = ""
				if q1.ARN.Region == "" && q2.ARN.Region != "" {
					q2.ARN.Region = ""
				}
				if reflect.DeepEqual(q1, q2) {
					return errors.New("duplicate queue configuration")
				}
			}
		}
		return c.Queues[len(c.Queues)-1].Validate(region, mgr)
	}

	return nil
}

func (c *NotificationConfig) IsEmpty() bool {
	return len(c.Lambdas) == 0 && len(c.Queues) == 0 && len(c.Topics) == 0
}

func (c *NotificationConfig) EventFilter() EventFilter {
	eventFilter := make(EventFilter)
	for _, q := range c.Queues {
		fm := FilterKeyMap{
			q.FilterKey(): NotifierIDSet{
				q.ARN.NotifierID(): struct{}{},
			},
		}
		ef := make(EventFilter)
		for _, event := range q.Events {
			for _, en := range event.Expand() {
				ef[en] = fm
			}
		}
		eventFilter.Add(ef)
	}

	return eventFilter
}

func ParseNotificationConfig(data []byte, region string, mgr *NotificationMgr) (*NotificationConfig, error) {
	var cfg NotificationConfig
	if err := xml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(region, mgr); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func IsNotificationService(s string) bool {
	for _, svc := range NotificationService {
		if svc == s {
			return true
		}
	}

	return false
}

func storeBucketNotification(vol *Volume, cfg *NotificationConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	if err = vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSNotification, data); err != nil {
		return err
	}

	vol.SetEventFilter(cfg.EventFilter())
	vol.metaLoader.storeNotification(cfg)

	return nil
}

func getBucketNotification(vol *Volume) (*NotificationConfig, error) {
	cfg, err := vol.loadNotification()
	if cfg == nil {
		cfg = &NotificationConfig{}
	}
	cfg.XMLNS = XMLNS

	return cfg, err
}

func deleteBucketNotification(vol *Volume) error {
	if err := vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSNotification); err != nil {
		return err
	}

	vol.SetEventFilter(EventFilter{})
	vol.metaLoader.storeNotification(nil)

	return nil
}
