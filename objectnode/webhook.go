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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type WebhookConfig struct {
	Endpoint      string          `json:"endpoint"`
	Authorization string          `json:"authorization"`
	Proxy         string          `json:"proxy"`
	Transport     TransportConfig `json:"transport"`
}

// FixConfig validates and fixes the configuration.
func (c *WebhookConfig) FixConfig() error {
	if c.Endpoint == "" {
		return errors.New("webhook: no endpoint found")
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("webhook: invalid endpoint '%s'", c.Endpoint)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return fmt.Errorf("webhook: unsupported scheme in '%s'", c.Endpoint)
	}

	return c.Transport.FixConfig()
}

// BuildClient creates a HTTP client.
// Make sure to call FixConfig before calling it to validate and fix the configuration.
func (c *WebhookConfig) BuildClient() (*http.Client, error) {
	transport, err := c.Transport.BuildTransport()
	if err != nil {
		return nil, err
	}

	if c.Proxy != "" {
		proxyURL, err := url.Parse(c.Proxy)
		if err != nil {
			return nil, err
		}
		if proxyURL != nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}

	return &http.Client{Transport: transport}, nil
}
