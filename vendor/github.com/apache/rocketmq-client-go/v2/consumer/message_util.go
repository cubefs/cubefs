/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consumer

import (
	"errors"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// CreateReplyMessage build reply message from the request message
func CreateReplyMessage(requestMessage *primitive.MessageExt, body []byte) (*primitive.Message, error) {
	if requestMessage == nil {
		return nil, errors.New("create reply message fail, requestMessage cannot be null")
	}

	cluster := requestMessage.GetProperty(primitive.PropertyCluster)
	replyTo := requestMessage.GetProperty(primitive.PropertyMessageReplyToClient)
	correlationId := requestMessage.GetProperty(primitive.PropertyCorrelationID)
	ttl := requestMessage.GetProperty(primitive.PropertyMessageTTL)

	if cluster == "" {
		return nil, fmt.Errorf("create reply message fail, requestMessage error, property[\"%s\"] is null", cluster)
	}

	var replayMessage primitive.Message

	replayMessage.UnmarshalProperties(body)
	replayMessage.Topic = internal.GetReplyTopic(cluster)
	replayMessage.WithProperty(primitive.PropertyMsgType, internal.ReplyMessageFlag)
	replayMessage.WithProperty(primitive.PropertyCorrelationID, correlationId)
	replayMessage.WithProperty(primitive.PropertyMessageReplyToClient, replyTo)
	replayMessage.WithProperty(primitive.PropertyMessageTTL, ttl)

	return &replayMessage, nil
}

func GetReplyToClient(msg *primitive.MessageExt) string {
	return msg.GetProperty(primitive.PropertyMessageReplyToClient)
}
