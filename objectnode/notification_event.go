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

type EventNotifier struct {
	async     bool
	notifiers map[NotifierID]Notifier

	sync.RWMutex
}

func NewEventNotifier() *EventNotifier {
	return &EventNotifier{
		notifiers: make(map[NotifierID]Notifier),
	}
}

func (n *EventNotifier) SetAsync(async bool) {
	n.Lock()
	defer n.Unlock()

	n.async = async
}

func (n *EventNotifier) IsAsync() bool {
	n.RLock()
	defer n.RUnlock()

	return n.async
}

func (n *EventNotifier) AddNotifier(notifiers ...Notifier) {
	n.Lock()
	defer n.Unlock()

	for _, notifier := range notifiers {
		n.notifiers[notifier.ID()] = notifier
	}
}

func (n *EventNotifier) DeleteNotifier(id NotifierID) {
	n.Lock()
	defer n.Unlock()

	notifier := n.notifiers[id]
	if notifier == nil {
		return
	}
	notifier.Close()
	delete(n.notifiers, id)
}

func (n *EventNotifier) GetNotifier(id NotifierID) Notifier {
	n.RLock()
	defer n.RUnlock()

	return n.notifiers[id]
}

func (n *EventNotifier) Close() error {
	n.Lock()
	defer n.Unlock()

	var ids []NotifierID
	for id, notifier := range n.notifiers {
		if notifier != nil {
			notifier.Close()
		}
		ids = append(ids, id)
	}

	for _, id := range ids {
		delete(n.notifiers, id)
	}

	return nil
}

func (n *EventNotifier) Send(idSet NotifierIDSet, event *Event) {
	data, err := json.Marshal(EventMessage{
		Records: []*Event{event},
	})
	if err != nil {
		log.LogErrorf("json marshal event message failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	for id := range idSet {
		if notifier := n.GetNotifier(id); notifier != nil {
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
	Name     EventName
	Bucket   string
	Region   string
	Key      string
	FileInfo *FSFileInfo

	RequestParams    map[string]string
	ResponseElements map[string]string
}

func extractEventRequestParams(r *http.Request) map[string]string {
	p := make(map[string]string)
	if r != nil {
		p = map[string]string{
			"host":         r.Host,
			"user-agent":   r.UserAgent(),
			"remote-ip":    getRequestIP(r),
			"principal-id": ParseRequestParam(r).AccessKey(),
		}
	}

	return p
}

func extractEventResponseElements(w http.ResponseWriter) map[string]string {
	e := make(map[string]string)
	if w != nil {
		e = map[string]string{
			"content-type":   w.Header().Get(ContentType),
			"content-length": w.Header().Get(ContentLength),
			"request-id":     w.Header().Get(XAmzRequestId),
		}
	}

	return e
}

func SendEventNotification(vol *Volume, params *EventParams) {
	if sysNotifier == nil || vol == nil || params == nil {
		return
	}

	notifierIDSet := vol.EventMatch(params.Name, params.Key)
	if len(notifierIDSet) <= 0 {
		return
	}

	reqParameters := map[string]string{
		"host":            params.RequestParams["host"],
		"userAgent":       params.RequestParams["user-agent"],
		"sourceIPAddress": params.RequestParams["remote-ip"],
	}

	respElements := map[string]string{
		"x-amz-request-id": params.RequestParams["request-id"],
	}
	if params.RequestParams["content-type"] != "" {
		respElements["content-type"] = params.RequestParams["content-type"]
	}
	if params.RequestParams["content-length"] != "" {
		respElements["content-length"] = params.RequestParams["content-length"]
	}

	eventTime := time.Now().UTC()
	event := &Event{
		EventVersion:      "2.0",
		EventSource:       "cubefs:s3",
		AwsRegion:         params.Region,
		EventTime:         eventTime.Format(ISO8601Layout),
		EventName:         strings.TrimPrefix(string(params.Name), "s3:"),
		UserIdentity:      IdentityRecord{PrincipalID: params.RequestParams["principal-id"]},
		RequestParameters: reqParameters,
		ResponseElements:  respElements,
		S3: S3Record{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: BucketRecord{
				Name:          params.Bucket,
				OwnerIdentity: IdentityRecord{PrincipalID: params.RequestParams["principal-id"]},
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

	if sysNotifier.IsAsync() {
		go sysNotifier.Send(notifierIDSet, event)
		return
	}
	sysNotifier.Send(notifierIDSet, event)
}
