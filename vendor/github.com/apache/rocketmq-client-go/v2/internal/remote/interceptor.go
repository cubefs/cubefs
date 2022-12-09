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

package remote

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"hash"
	"sort"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	signature     = "Signature"
	accessKey     = "AccessKey"
	securityToken = "SecurityToken"
	keyFile       = "KEY_FILE"
	// System.getProperty("rocketmq.client.keyFile", System.getProperty("user.home") + File.separator + "key");
)

func ACLInterceptor(credentials primitive.Credentials) primitive.Interceptor {
	return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
		cmd := req.(*RemotingCommand)
		m := make(map[string]string)
		order := make([]string, 1)
		m[accessKey] = credentials.AccessKey
		order[0] = accessKey
		if credentials.SecurityToken != "" {
			m[securityToken] = credentials.SecurityToken
		}
		for k, v := range cmd.ExtFields {
			m[k] = v
			order = append(order, k)
		}
		sort.Slice(order, func(i, j int) bool {
			return strings.Compare(order[i], order[j]) < 0
		})
		content := ""
		for idx := range order {
			content += m[order[idx]]
		}
		buf := make([]byte, len(content)+len(cmd.Body))
		copy(buf, []byte(content))
		copy(buf[len(content):], cmd.Body)

		cmd.ExtFields[signature] = calculateSignature(buf, []byte(credentials.SecretKey))
		cmd.ExtFields[accessKey] = credentials.AccessKey

		// The SecurityToken value is unnecessary, user can choose this one.
		if credentials.SecurityToken != "" {
			cmd.ExtFields[securityToken] = credentials.SecurityToken
		}
		err := next(ctx, req, reply)
		return err
	}
}

func calculateSignature(data, sk []byte) string {
	mac := hmac.New(func() hash.Hash {
		return sha1.New()
	}, sk)
	mac.Write(data)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
