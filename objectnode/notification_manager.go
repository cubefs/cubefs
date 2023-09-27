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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type Notifier interface {
	ID() NotifierID
	Name() string
	Send(data []byte) error
	Close() error
}

type EventNotificationConfig struct {
	Async bool `json:"async"`
	// The key of map is a unique identifier of notifier
	Kafka   map[string]KafkaNotifierConfig   `json:"kafka"`
	Redis   map[string]RedisNotifierConfig   `json:"redis"`
	Nsq     map[string]NsqNotifierConfig     `json:"nsq"`
	Nats    map[string]NatsNotifierConfig    `json:"nats"`
	Mqtt    map[string]MqttNotifierConfig    `json:"mqtt"`
	Webhook map[string]WebhookNotifierConfig `json:"webhook"`
}

type NotificationMgr struct {
	async     bool
	notifiers map[NotifierID]Notifier

	sync.RWMutex
}

func NewNotificationMgr() *NotificationMgr {
	return &NotificationMgr{
		notifiers: make(map[NotifierID]Notifier),
	}
}

func (m *NotificationMgr) SetAsync(async bool) {
	m.Lock()
	defer m.Unlock()

	m.async = async
}

func (m *NotificationMgr) IsAsync() bool {
	m.RLock()
	defer m.RUnlock()

	return m.async
}

func (m *NotificationMgr) AddNotifier(notifiers ...Notifier) {
	m.Lock()
	defer m.Unlock()

	for _, notifier := range notifiers {
		m.notifiers[notifier.ID()] = notifier
	}
}

func (m *NotificationMgr) DelNotifier(id NotifierID) {
	m.Lock()
	defer m.Unlock()

	notifier := m.notifiers[id]
	if notifier == nil {
		return
	}
	notifier.Close()
	delete(m.notifiers, id)
}

func (m *NotificationMgr) GetNotifier(id NotifierID) Notifier {
	m.RLock()
	defer m.RUnlock()

	return m.notifiers[id]
}

func (m *NotificationMgr) Close() error {
	m.Lock()
	defer m.Unlock()

	var ids []NotifierID
	for _, notifier := range m.notifiers {
		if notifier != nil {
			ids = append(ids, notifier.ID())
		}
	}

	for _, id := range ids {
		m.notifiers[id].Close()
		delete(m.notifiers, id)
	}

	return nil
}

func (m *NotificationMgr) Send(idSet NotifierIDSet, event *Event) {
	data, err := json.Marshal(EventMessage{
		Records: []*Event{event},
	})
	if err != nil {
		log.LogErrorf("json marshal event message failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	for id := range idSet {
		if notifier := m.GetNotifier(id); notifier != nil {
			wg.Add(1)
			go func(notifier Notifier) {
				defer wg.Done()
				if err = notifier.Send(data); err != nil {
					log.LogErrorf("notifier '%s' send event failed: %v", notifier.Name(), err)
				}
			}(notifier)
		}
	}
	wg.Wait()
}

type IdentityRecord struct {
	PrincipalID string `json:"principalId"`
}

type BucketRecord struct {
	Name          string         `json:"name"`
	OwnerIdentity IdentityRecord `json:"ownerIdentity"`
	ARN           string         `json:"arn"`
}

type ObjectRecord struct {
	Key          string            `json:"key,omitempty"`
	Size         int64             `json:"size,omitempty"`
	ETag         string            `json:"eTag,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	UserMetadata map[string]string `json:"userMetadata,omitempty"`
	VersionID    string            `json:"versionId,omitempty"`
	Sequencer    string            `json:"sequencer,omitempty"`
}

type S3Record struct {
	SchemaVersion   string       `json:"s3SchemaVersion"`
	ConfigurationID string       `json:"configurationId"`
	Bucket          BucketRecord `json:"bucket"`
	Object          ObjectRecord `json:"object"`
}

type Event struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         string            `json:"eventTime"`
	EventName         string            `json:"eventName"`
	UserIdentity      IdentityRecord    `json:"userIdentity"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                S3Record          `json:"s3"`
}

type EventMessage struct {
	Records []*Event `json:"Records"`
}

type EventParams struct {
	Request  *http.Request
	Response http.ResponseWriter

	Name     EventName
	Bucket   string
	Region   string
	Key      string
	FileInfo *FSFileInfo
}

func SendEventNotification(notifyMgr *NotificationMgr, vol *Volume, params *EventParams) {
	if notifyMgr == nil || vol == nil || params == nil || params.Request == nil || params.Response == nil {
		return
	}

	notifierIDSet := vol.EventMatch(params.Name, params.Key)
	if len(notifierIDSet) <= 0 {
		return
	}

	reqParameters := map[string]string{
		"host":            params.Request.Host,
		"userAgent":       params.Request.UserAgent(),
		"sourceIPAddress": getRequestIP(params.Request),
	}

	reqParam := ParseRequestParam(params.Request)
	respElements := map[string]string{
		"x-amz-request-id": reqParam.RequestID(),
	}
	if params.Response.Header().Get(ContentLength) != "" {
		respElements["content-length"] = params.Response.Header().Get(ContentLength)
	}
	if params.Response.Header().Get(ContentType) != "" {
		respElements["content-type"] = params.Response.Header().Get(ContentType)
	}

	eventTime := time.Now().UTC()
	event := &Event{
		EventVersion:      "2.0",
		EventSource:       "cubefs:s3",
		AwsRegion:         params.Region,
		EventTime:         eventTime.Format(ISO8601Layout),
		EventName:         strings.TrimPrefix(string(params.Name), "s3:"),
		UserIdentity:      IdentityRecord{PrincipalID: reqParam.AccessKey()},
		RequestParameters: reqParameters,
		ResponseElements:  respElements,
		S3: S3Record{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: BucketRecord{
				Name:          params.Bucket,
				OwnerIdentity: IdentityRecord{PrincipalID: reqParam.AccessKey()},
				ARN:           "arn:aws:s3:::" + params.Bucket,
			},
			Object: ObjectRecord{
				Key:       url.QueryEscape(params.Key),
				Sequencer: fmt.Sprintf("%X", eventTime.UnixNano()),
			},
		},
	}

	if params.FileInfo != nil {
		event.S3.Object.Size = params.FileInfo.Size
		event.S3.Object.ETag = params.FileInfo.ETag
		event.S3.Object.ContentType = params.FileInfo.MIMEType
		if len(params.FileInfo.Metadata) > 0 {
			event.S3.Object.UserMetadata = make(map[string]string)
			for k, v := range params.FileInfo.Metadata {
				event.S3.Object.UserMetadata[k] = v
			}
		}
	}

	if notifyMgr.IsAsync() {
		go notifyMgr.Send(notifierIDSet, event)
		return
	}
	notifyMgr.Send(notifierIDSet, event)
}
