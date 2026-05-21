// Copyright 2026 The CubeFS Authors.
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

package tasks

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// WriteHistoryJSONL streams every history record into w as newline-
// delimited JSON ("application/x-ndjson"), one Record per line. The caller
// is expected to have set Content-Type + Content-Disposition headers
// already; this helper writes the body only.
//
// The `since` parameter is forwarded to Store.ListHistory; pass the zero
// time to dump every record currently in history.
//
// Errors are bubbled up unchanged. Because JSONL is line-oriented, a
// partial write surfaces to the client as one fewer line — clients that
// parse line-by-line can recover gracefully. Callers that need atomic
// semantics should buffer first.
func WriteHistoryJSONL(ctx context.Context, store Store, w io.Writer, since time.Time) error {
	recs, err := store.ListHistory(ctx, since)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	// json.Encoder.Encode writes a trailing newline after each value —
	// exactly the framing JSONL ("ndjson") requires.
	for _, r := range recs {
		// Cooperative cancellation between records keeps large exports
		// interruptible without resorting to chunked I/O abort tricks.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}
