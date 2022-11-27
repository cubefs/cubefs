// Copyright 2020 The CubeFS Authors.
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
	"time"
)

type sample struct {
	raw       string
	etagValue ETagValue
	etag      string
}

var samples = []sample{
	{
		raw: "41f9ede9b03b89d80f3a8460d7792ff6",
		etagValue: ETagValue{
			Value:   "41f9ede9b03b89d80f3a8460d7792ff6",
			PartNum: 0,
			TS:      time.Unix(0, 0)},
		etag: "41f9ede9b03b89d80f3a8460d7792ff6",
	},
	{
		raw: "41f9ede9b03b89d80f3a8460d7792ff6-23",
		etagValue: ETagValue{
			Value:   "41f9ede9b03b89d80f3a8460d7792ff6",
			PartNum: 23,
			TS:      time.Unix(0, 0)},
		etag: "41f9ede9b03b89d80f3a8460d7792ff6-23",
	},
	{
		raw: "41f9ede9b03b89d80f3a8460d7792ff6:1588562233",
		etagValue: ETagValue{
			Value:   "41f9ede9b03b89d80f3a8460d7792ff6",
			PartNum: 0,
			TS:      time.Unix(1588562233, 0)},
		etag: "41f9ede9b03b89d80f3a8460d7792ff6",
	},
	{
		raw: "41f9ede9b03b89d80f3a8460d7792ff6-23:1588562233",
		etagValue: ETagValue{
			Value:   "41f9ede9b03b89d80f3a8460d7792ff6",
			PartNum: 23,
			TS:      time.Unix(1588562233, 0)},
		etag: "41f9ede9b03b89d80f3a8460d7792ff6-23",
	},
}

func TestParseETagValue(t *testing.T) {
	for i, sample := range samples {
		if etagValue := ParseETagValue(sample.raw); etagValue != sample.etagValue {
			t.Fatalf("result mismatch: index(%v) expect(%v) actual(%v)", i, sample.etagValue, etagValue)
		}
	}
}

func TestETagValue_Encode(t *testing.T) {
	for i, sample := range samples {
		if encoded := sample.etagValue.Encode(); encoded != sample.raw {
			t.Fatalf("result mismatch: index(%v) expect(%v) actual(%v)", i, sample.raw, encoded)
		}
	}
}

func TestETagValue_ETag(t *testing.T) {
	for i, sample := range samples {
		if etag := sample.etagValue.ETag(); etag != sample.etag {
			t.Fatalf("result mismatch: index(%v) expect(%v) actual(%v)", i, sample.etag, etag)
		}
	}
}
