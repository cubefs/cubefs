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
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestParseNotificationConfig(t *testing.T) {
	server1 := CreateWebhookTestServer()
	defer server1.Close()
	wconf := WebhookNotifierConfig{}
	wconf.Endpoint = server1.URL
	notifier1, err := NewWebhookNotifier("cubefs", wconf)
	require.NoError(t, err)

	server2 := miniredis.RunT(t)
	defer server2.Close()
	rconf := RedisNotifierConfig{}
	rconf.Address = server2.Addr()
	rconf.Key = "redis-notifier-unit-test"
	notifier2, err := NewRedisNotifier("cubefs", rconf)
	require.NoError(t, err)

	eventNotifier := NewEventNotifier()
	eventNotifier.AddNotifier(notifier1, notifier2)

	testCases := []struct {
		Value               string
		ExpectedErrContains string
	}{
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						 <Id>1</Id>
						 <Filter>
								<S3Key>
									<FilterRule>
										<Name>prefix</Name>
										<Value>images/</Value>
									</FilterRule>
								</S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					  <QueueConfiguration>
						 <Id>2</Id>
						 <Filter>
								<S3Key>
									<FilterRule>
										<Name>prefix</Name>
										<Value>logs/</Value>
									</FilterRule>
								</S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:redis</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						 <Id>1</Id>
						 <Filter>
								<S3Key>
									<FilterRule>
										<Name>prefix</Name>
										<Value>images/</Value>
									</FilterRule>
								</S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					  <QueueConfiguration>
						 <Id>2</Id>
						 <Filter>
								<S3Key>
									<FilterRule>
										<Name>prefix</Name>
										<Value>images/</Value>
									</FilterRule>
								</S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "duplicate queue",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:other-service</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "not found",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:other:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "invalid ARN",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:other:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "invalid ARN",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test::webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "invalid ARN",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "missing event",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
						 <Event>s3:ObjectCreated:Other</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "unsupported event",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "duplicate event",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>invalid name</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>suffix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "invalid filter rule name",
		},
		{
			Value: `<NotificationConfiguration>
					  <QueueConfiguration>
						  <Id>1</Id>
						  <Filter>
							  <S3Key>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>images/</Value>
								  </FilterRule>
								  <FilterRule>
									  <Name>prefix</Name>
									  <Value>jpg</Value>
								  </FilterRule>
							  </S3Key>
						 </Filter>
						 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
						 <Event>s3:ObjectCreated:Put</Event>
					  </QueueConfiguration>
					</NotificationConfiguration>`,
			ExpectedErrContains: "duplicate filter rule name",
		},
	}

	for _, tc := range testCases {
		_, err = ParseNotificationConfig([]byte(tc.Value), "cfs-test", eventNotifier)
		if err != nil {
			require.ErrorContains(t, err, tc.ExpectedErrContains)
		}
	}
}

func TestEventFilterMatch(t *testing.T) {
	server := CreateWebhookTestServer()
	defer server.Close()
	conf := WebhookNotifierConfig{}
	conf.Endpoint = server.URL
	notifier, err := NewWebhookNotifier("cubefs", conf)
	require.NoError(t, err)

	eventNotifier := NewEventNotifier()
	eventNotifier.AddNotifier(notifier)

	config := `<NotificationConfiguration>
			  <QueueConfiguration>
				 <Id>1</Id>
				 <Filter>
						<S3Key>
							<FilterRule>
								<Name>prefix</Name>
								<Value>cfs</Value>
							</FilterRule>
							<FilterRule>
								<Name>suffix</Name>
								<Value>jpg</Value>
						    </FilterRule>
						</S3Key>
				 </Filter>
				 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
				 <Event>s3:ObjectCreated:*</Event>
				 <Event>s3:ObjectAccessed:Get</Event>
			  </QueueConfiguration>
			  <QueueConfiguration>
				 <Id>2</Id>
				 <Filter>
						<S3Key>
							<FilterRule>
								<Name>suffix</Name>
								<Value>.json</Value>
						    </FilterRule>
						</S3Key>
				 </Filter>
				 <Queue>arn:cfs:sqs:cfs-test:cubefs:webhook</Queue>
				 <Event>s3:ObjectRemoved:*</Event>
			  </QueueConfiguration>
			</NotificationConfiguration>`

	notification, err := ParseNotificationConfig([]byte(config), "cfs-test", eventNotifier)
	require.NoError(t, err)
	eventFilter := notification.EventFilter()

	testCases := []struct {
		Key      string
		Event    EventName
		Expected bool
	}{
		{
			Key:      "cfs-test.jpg",
			Event:    ObjectAccessedGet,
			Expected: true,
		},
		{
			Key:      "cfs-test.jpg",
			Event:    ObjectCreatedPut,
			Expected: true,
		},
		{
			Key:      "cfa-test.jpg",
			Event:    ObjectAccessedGet,
			Expected: false,
		},
		{
			Key:      "cfs-test.gif",
			Event:    ObjectAccessedGet,
			Expected: false,
		},
		{
			Key:      "cfa-test.gif",
			Event:    ObjectAccessedGet,
			Expected: false,
		},
		{
			Key:      "cfs-test.jpg",
			Event:    ObjectRemovedDelete,
			Expected: false,
		},
		{
			Key:      "cfs-test.json",
			Event:    ObjectRemovedDelete,
			Expected: true,
		},
		{
			Key:      "cfs-test.json",
			Event:    ObjectAccessedHead,
			Expected: false,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, len(eventFilter.Match(tc.Event, tc.Key)) > 0, tc.Expected)
	}
}
