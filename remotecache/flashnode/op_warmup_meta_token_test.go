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

package flashnode

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func newTestFlashNodeForToken(totalToken int) *FlashNode {
	return &FlashNode{
		warmupMetaTotalToken: totalToken,
		currentWarmUpWorkers: make(map[string]int64),
	}
}

// callOpApplyWarmupMetaToken is a test helper that invokes opApplyWarmupMetaToken
// over an in-memory net.Pipe and returns the response packet. It constructs a
// proto.Packet with the given clientId (set via Arg/ArgLen) and reqData (the
// first byte of which is the request type), then spawns a goroutine to read
// the response from the client side of the pipe while the server side processes
// the request synchronously.
func callOpApplyWarmupMetaToken(t *testing.T, f *FlashNode, clientId string, reqData []byte) *proto.Packet {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	p := proto.NewPacket()
	p.Opcode = proto.OpApplyWarmupMetaToken
	if clientId != "" {
		p.Arg = []byte(clientId)
		p.ArgLen = uint32(len(clientId))
	}
	p.Data = reqData
	p.Size = uint32(len(reqData))

	done := make(chan error, 1)
	resp := proto.NewPacket()
	go func() {
		done <- resp.ReadFromConn(clientConn, 5)
	}()

	err := f.opApplyWarmupMetaToken(serverConn, p)
	require.NoError(t, err)

	require.NoError(t, <-done)
	return resp
}

// TestOpApplyWarmupMetaToken_EmptyRequestData verifies that the function returns
// OpErr with "empty request data" when p.Data is nil or has zero length.
func TestOpApplyWarmupMetaToken_EmptyRequestData(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-1", nil)
	require.Equal(t, proto.OpErr, resp.ResultCode)
	require.Contains(t, string(resp.Data), "empty request data")

	resp = callOpApplyWarmupMetaToken(t, f, "client-2", []byte{})
	require.Equal(t, proto.OpErr, resp.ResultCode)
	require.Contains(t, string(resp.Data), "empty request data")
}

// TestOpApplyWarmupMetaToken_ApplySuccess verifies a single client can
// successfully acquire a warmup meta token when capacity is available.
func TestOpApplyWarmupMetaToken_ApplySuccess(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-1", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, uint32(1), resp.Size)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers["client-1"]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_ApplyMultipleClients verifies that multiple
// distinct clients can each acquire a token up to the configured limit.
func TestOpApplyWarmupMetaToken_ApplyMultipleClients(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	for i := 0; i < 3; i++ {
		clientId := fmt.Sprintf("client-%d", i)
		resp := callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, proto.OpOk, resp.ResultCode)
		require.Equal(t, []byte{1}, resp.Data[:1], "client %d should succeed", i)
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 3, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_ApplyExceedLimit verifies that when the number of
// active workers reaches warmupMetaTotalToken, subsequent apply requests are
// rejected (response data byte = 0).
func TestOpApplyWarmupMetaToken_ApplyExceedLimit(t *testing.T) {
	f := newTestFlashNodeForToken(2)

	resp := callOpApplyWarmupMetaToken(t, f, "client-1", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-2", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-3", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 2, len(f.currentWarmUpWorkers))
	_, exists := f.currentWarmUpWorkers["client-3"]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.False(t, exists)
}

// TestOpApplyWarmupMetaToken_ApplyWithTokenLimitOne tests the boundary case
// where only 1 token is configured; the second client must be rejected.
func TestOpApplyWarmupMetaToken_ApplyWithTokenLimitOne(t *testing.T) {
	f := newTestFlashNodeForToken(1)

	resp := callOpApplyWarmupMetaToken(t, f, "client-A", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-B", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 1, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_ApplyDuplicateClient verifies that applying with
// the same clientId twice overwrites the timestamp but does not increase the
// worker count (map semantics).
func TestOpApplyWarmupMetaToken_ApplyDuplicateClient(t *testing.T) {
	f := newTestFlashNodeForToken(2)

	resp := callOpApplyWarmupMetaToken(t, f, "client-dup", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-dup", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 1, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_RenewSuccess verifies that an existing token
// holder can successfully renew (refresh) its lease.
func TestOpApplyWarmupMetaToken_RenewSuccess(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-renew", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-renew", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers["client-renew"]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_RenewNotFound verifies that renewing a token for
// a clientId that never applied returns failure (data byte = 0).
func TestOpApplyWarmupMetaToken_RenewNotFound(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "ghost-client", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])
}

// TestOpApplyWarmupMetaToken_RenewMultipleTimes verifies that renewing the
// same token repeatedly always succeeds and does not alter the worker map size.
func TestOpApplyWarmupMetaToken_RenewMultipleTimes(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-multi-renew", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	for i := 0; i < 5; i++ {
		resp = callOpApplyWarmupMetaToken(t, f, "client-multi-renew", []byte{proto.WarmupMetaTokenRenew})
		require.Equal(t, proto.OpOk, resp.ResultCode)
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 1, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_ReleaseSuccess verifies that a client holding a
// token can release it, removing itself from the active workers map.
func TestOpApplyWarmupMetaToken_ReleaseSuccess(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-release", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-release", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers["client-release"]
	count := len(f.currentWarmUpWorkers)
	f.currentWarmUpWorkerMutex.RUnlock()
	require.False(t, exists)
	require.Equal(t, 0, count)
}

// TestOpApplyWarmupMetaToken_ReleaseNotFound verifies that releasing a token
// for a non-existent clientId returns failure (data byte = 0).
func TestOpApplyWarmupMetaToken_ReleaseNotFound(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "no-such-client", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])
}

// TestOpApplyWarmupMetaToken_ReleaseAfterRelease verifies that releasing the
// same token twice results in success on the first call and failure on the second.
func TestOpApplyWarmupMetaToken_ReleaseAfterRelease(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	callOpApplyWarmupMetaToken(t, f, "client-x", []byte{proto.WarmupMetaTokenApply})

	resp := callOpApplyWarmupMetaToken(t, f, "client-x", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-x", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, []byte{0}, resp.Data[:1])
}

// TestOpApplyWarmupMetaToken_InvalidRequestType verifies that any request type
// outside the known set (Apply=1, Renew=2, Release=3) triggers OpErr with
// "invalid request type".
func TestOpApplyWarmupMetaToken_InvalidRequestType(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "client-bad", []byte{0})
	require.Equal(t, proto.OpErr, resp.ResultCode)
	require.Contains(t, string(resp.Data), "invalid request type")

	resp = callOpApplyWarmupMetaToken(t, f, "client-bad", []byte{99})
	require.Equal(t, proto.OpErr, resp.ResultCode)

	resp = callOpApplyWarmupMetaToken(t, f, "client-bad", []byte{255})
	require.Equal(t, proto.OpErr, resp.ResultCode)
}

// TestOpApplyWarmupMetaToken_EmptyClientId verifies that an empty clientId
// (ArgLen=0) is treated as a valid key "" and the token is still granted.
func TestOpApplyWarmupMetaToken_EmptyClientId(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers[""]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_ApplyThenReleaseThenApplyAgain tests the scenario
// where a token slot is freed and a previously-rejected client can now acquire it.
func TestOpApplyWarmupMetaToken_ApplyThenReleaseThenApplyAgain(t *testing.T) {
	f := newTestFlashNodeForToken(1)

	resp := callOpApplyWarmupMetaToken(t, f, "client-cycle", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "another-client", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{0}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "client-cycle", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "another-client", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 1, len(f.currentWarmUpWorkers))
	_, exists := f.currentWarmUpWorkers["another-client"]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_RenewAfterRelease verifies that renewing a token
// that was previously released returns failure since the entry no longer exists.
func TestOpApplyWarmupMetaToken_RenewAfterRelease(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	callOpApplyWarmupMetaToken(t, f, "client-rr", []byte{proto.WarmupMetaTokenApply})
	callOpApplyWarmupMetaToken(t, f, "client-rr", []byte{proto.WarmupMetaTokenRelease})

	resp := callOpApplyWarmupMetaToken(t, f, "client-rr", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])
}

// TestOpApplyWarmupMetaToken_ConcurrentApply verifies thread-safety by spawning
// 10 goroutines that each try to apply for a token with a limit of 5; exactly
// 5 should succeed and 5 should fail.
func TestOpApplyWarmupMetaToken_ConcurrentApply(t *testing.T) {
	f := newTestFlashNodeForToken(5)

	var wg sync.WaitGroup
	results := make([]byte, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientId := fmt.Sprintf("concurrent-client-%d", idx)
			resp := callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenApply})
			require.Equal(t, proto.OpOk, resp.ResultCode)
			results[idx] = resp.Data[0]
		}(i)
	}
	wg.Wait()

	successCount := 0
	failCount := 0
	for _, r := range results {
		if r == 1 {
			successCount++
		} else {
			failCount++
		}
	}
	require.Equal(t, 5, successCount)
	require.Equal(t, 5, failCount)

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 5, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_ConcurrentRenew verifies that multiple goroutines
// can concurrently renew the same token without data races or failures.
func TestOpApplyWarmupMetaToken_ConcurrentRenew(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	callOpApplyWarmupMetaToken(t, f, "renew-conc", []byte{proto.WarmupMetaTokenApply})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := callOpApplyWarmupMetaToken(t, f, "renew-conc", []byte{proto.WarmupMetaTokenRenew})
			require.Equal(t, proto.OpOk, resp.ResultCode)
			require.Equal(t, []byte{1}, resp.Data[:1])
		}()
	}
	wg.Wait()
}

// TestOpApplyWarmupMetaToken_ConcurrentRelease verifies that when multiple
// goroutines try to release the same token concurrently, exactly one succeeds.
func TestOpApplyWarmupMetaToken_ConcurrentRelease(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	callOpApplyWarmupMetaToken(t, f, "release-conc", []byte{proto.WarmupMetaTokenApply})

	var wg sync.WaitGroup
	results := make([]byte, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp := callOpApplyWarmupMetaToken(t, f, "release-conc", []byte{proto.WarmupMetaTokenRelease})
			require.Equal(t, proto.OpOk, resp.ResultCode)
			results[idx] = resp.Data[0]
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, r := range results {
		if r == 1 {
			successCount++
		}
	}
	require.Equal(t, 1, successCount)
}

// TestOpApplyWarmupMetaToken_ConcurrentMixed exercises a full concurrent
// lifecycle: apply 3 clients in parallel, renew them in parallel, then release
// them in parallel, verifying no workers remain at the end.
func TestOpApplyWarmupMetaToken_ConcurrentMixed(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientId := fmt.Sprintf("mixed-%d", idx)
			callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenApply})
		}(i)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientId := fmt.Sprintf("mixed-%d", idx)
			callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenRenew})
		}(i)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientId := fmt.Sprintf("mixed-%d", idx)
			callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenRelease})
		}(i)
	}
	wg.Wait()

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 0, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_ZeroTokenLimit verifies that with a token limit
// of 0, all apply requests are rejected immediately.
func TestOpApplyWarmupMetaToken_ZeroTokenLimit(t *testing.T) {
	f := newTestFlashNodeForToken(0)

	resp := callOpApplyWarmupMetaToken(t, f, "client-zero", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{0}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 0, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_LargeTokenLimit verifies that a high token limit
// (1000) allows many clients to acquire tokens without rejection.
func TestOpApplyWarmupMetaToken_LargeTokenLimit(t *testing.T) {
	f := newTestFlashNodeForToken(1000)

	for i := 0; i < 100; i++ {
		clientId := fmt.Sprintf("large-limit-client-%d", i)
		resp := callOpApplyWarmupMetaToken(t, f, clientId, []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, proto.OpOk, resp.ResultCode)
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 100, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_LongClientId verifies that a very long clientId
// (256 characters) is handled correctly without truncation or error.
func TestOpApplyWarmupMetaToken_LongClientId(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	longId := ""
	for i := 0; i < 256; i++ {
		longId += "a"
	}

	resp := callOpApplyWarmupMetaToken(t, f, longId, []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers[longId]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_SpecialCharClientId verifies that clientIds
// containing special characters (slashes, colons, @, spaces, tabs) are stored
// and retrieved correctly.
func TestOpApplyWarmupMetaToken_SpecialCharClientId(t *testing.T) {
	f := newTestFlashNodeForToken(5)
	specialIds := []string{
		"client/with/slashes",
		"client:with:colons",
		"client@host",
		"client with spaces",
		"client\twith\ttabs",
	}

	for _, id := range specialIds {
		resp := callOpApplyWarmupMetaToken(t, f, id, []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, proto.OpOk, resp.ResultCode)
		require.Equal(t, []byte{1}, resp.Data[:1], "client id: %s", id)
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 5, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_FullLifecycle exercises the complete token
// lifecycle: apply until full, renew existing tokens, reject over-capacity
// renew, release one slot, and re-apply with a new client.
func TestOpApplyWarmupMetaToken_FullLifecycle(t *testing.T) {
	f := newTestFlashNodeForToken(2)

	resp := callOpApplyWarmupMetaToken(t, f, "lifecycle-A", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-B", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-C", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{0}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-A", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-B", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-C", []byte{proto.WarmupMetaTokenRenew})
	require.Equal(t, []byte{0}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-A", []byte{proto.WarmupMetaTokenRelease})
	require.Equal(t, []byte{1}, resp.Data[:1])

	resp = callOpApplyWarmupMetaToken(t, f, "lifecycle-C", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 2, len(f.currentWarmUpWorkers))
	_, existsB := f.currentWarmUpWorkers["lifecycle-B"]
	_, existsC := f.currentWarmUpWorkers["lifecycle-C"]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, existsB)
	require.True(t, existsC)
}

// TestOpApplyWarmupMetaToken_WriteConnError verifies that when the client
// connection is already closed, WriteToConn fails and the function returns
// an error.
func TestOpApplyWarmupMetaToken_WriteConnError(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	serverConn, clientConn := net.Pipe()
	clientConn.Close()

	p := proto.NewPacket()
	p.Opcode = proto.OpApplyWarmupMetaToken
	p.Arg = []byte("err-client")
	p.ArgLen = uint32(len(p.Arg))
	p.Data = []byte{proto.WarmupMetaTokenApply}
	p.Size = 1

	err := f.opApplyWarmupMetaToken(serverConn, p)
	require.Error(t, err)
	serverConn.Close()
}

// TestOpApplyWarmupMetaToken_WriteConnErrorOnEmptyData verifies that the
// "empty request data" error path also returns an error when the connection
// is closed and WriteToConn fails.
func TestOpApplyWarmupMetaToken_WriteConnErrorOnEmptyData(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	serverConn, clientConn := net.Pipe()
	clientConn.Close()

	p := proto.NewPacket()
	p.Opcode = proto.OpApplyWarmupMetaToken
	p.Arg = []byte("err-client-2")
	p.ArgLen = uint32(len(p.Arg))
	p.Data = nil
	p.Size = 0

	err := f.opApplyWarmupMetaToken(serverConn, p)
	require.Error(t, err)
	serverConn.Close()
}

// TestOpApplyWarmupMetaToken_NoArgLen verifies the behavior when ArgLen is 0
// (no client ID in Arg), resulting in an empty string clientId being used as
// the map key.
func TestOpApplyWarmupMetaToken_NoArgLen(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	p := proto.NewPacket()
	p.Opcode = proto.OpApplyWarmupMetaToken
	p.ArgLen = 0
	p.Data = []byte{proto.WarmupMetaTokenApply}
	p.Size = 1

	done := make(chan error, 1)
	resp := proto.NewPacket()
	go func() {
		done <- resp.ReadFromConn(clientConn, 5)
	}()

	err := f.opApplyWarmupMetaToken(serverConn, p)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	_, exists := f.currentWarmUpWorkers[""]
	f.currentWarmUpWorkerMutex.RUnlock()
	require.True(t, exists)
}

// TestOpApplyWarmupMetaToken_ApplyReleaseAllThenReapply verifies that after
// all tokens are released, the full capacity becomes available again for new
// clients.
func TestOpApplyWarmupMetaToken_ApplyReleaseAllThenReapply(t *testing.T) {
	f := newTestFlashNodeForToken(2)
	clients := []string{"batch-1", "batch-2"}

	for _, c := range clients {
		resp := callOpApplyWarmupMetaToken(t, f, c, []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	for _, c := range clients {
		resp := callOpApplyWarmupMetaToken(t, f, c, []byte{proto.WarmupMetaTokenRelease})
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 0, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()

	newClients := []string{"new-batch-1", "new-batch-2"}
	for _, c := range newClients {
		resp := callOpApplyWarmupMetaToken(t, f, c, []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 2, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_RequestTypeEdgeCases iterates over various invalid
// request type byte values and confirms they all result in OpErr.
func TestOpApplyWarmupMetaToken_RequestTypeEdgeCases(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	edgeCases := []byte{0, 4, 5, 10, 50, 100, 127, 128, 200, 254, 255}
	for _, reqType := range edgeCases {
		if reqType == proto.WarmupMetaTokenApply || reqType == proto.WarmupMetaTokenRenew || reqType == proto.WarmupMetaTokenRelease {
			continue
		}
		resp := callOpApplyWarmupMetaToken(t, f, "edge-client", []byte{reqType})
		require.Equal(t, proto.OpErr, resp.ResultCode, "request type %d should be invalid", reqType)
	}
}

// TestOpApplyWarmupMetaToken_ApplyAtExactLimit tests the boundary: fill tokens
// to exactly the limit, verify overflow is rejected, release one, and confirm
// the freed slot can be claimed.
func TestOpApplyWarmupMetaToken_ApplyAtExactLimit(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	for i := 0; i < 3; i++ {
		resp := callOpApplyWarmupMetaToken(t, f, fmt.Sprintf("exact-%d", i), []byte{proto.WarmupMetaTokenApply})
		require.Equal(t, []byte{1}, resp.Data[:1])
	}

	resp := callOpApplyWarmupMetaToken(t, f, "exact-overflow", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{0}, resp.Data[:1])

	callOpApplyWarmupMetaToken(t, f, "exact-0", []byte{proto.WarmupMetaTokenRelease})

	resp = callOpApplyWarmupMetaToken(t, f, "exact-new", []byte{proto.WarmupMetaTokenApply})
	require.Equal(t, []byte{1}, resp.Data[:1])

	f.currentWarmUpWorkerMutex.RLock()
	require.Equal(t, 3, len(f.currentWarmUpWorkers))
	f.currentWarmUpWorkerMutex.RUnlock()
}

// TestOpApplyWarmupMetaToken_MultiByteRequestData verifies that only the first
// byte of reqData is used as the request type; trailing bytes are ignored.
func TestOpApplyWarmupMetaToken_MultiByteRequestData(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	resp := callOpApplyWarmupMetaToken(t, f, "multi-byte", []byte{proto.WarmupMetaTokenApply, 0xFF, 0xAA})
	require.Equal(t, proto.OpOk, resp.ResultCode)
	require.Equal(t, []byte{1}, resp.Data[:1])
}

// TestOpApplyWarmupMetaToken_ConcurrentApplyAndRelease runs concurrent releases
// of existing tokens and applies of new tokens to verify there are no races and
// the final count respects the token limit.
func TestOpApplyWarmupMetaToken_ConcurrentApplyAndRelease(t *testing.T) {
	f := newTestFlashNodeForToken(5)

	for i := 0; i < 5; i++ {
		callOpApplyWarmupMetaToken(t, f, fmt.Sprintf("ar-client-%d", i), []byte{proto.WarmupMetaTokenApply})
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			callOpApplyWarmupMetaToken(t, f, fmt.Sprintf("ar-client-%d", idx), []byte{proto.WarmupMetaTokenRelease})
		}(i)
		go func(idx int) {
			defer wg.Done()
			callOpApplyWarmupMetaToken(t, f, fmt.Sprintf("ar-new-client-%d", idx), []byte{proto.WarmupMetaTokenApply})
		}(i)
	}
	wg.Wait()

	f.currentWarmUpWorkerMutex.RLock()
	count := len(f.currentWarmUpWorkers)
	f.currentWarmUpWorkerMutex.RUnlock()
	require.LessOrEqual(t, count, 5)
}

// TestOpApplyWarmupMetaToken_RenewDoesNotChangeCount verifies that repeated
// renewals never increase or decrease the active worker count.
func TestOpApplyWarmupMetaToken_RenewDoesNotChangeCount(t *testing.T) {
	f := newTestFlashNodeForToken(3)

	callOpApplyWarmupMetaToken(t, f, "stable-1", []byte{proto.WarmupMetaTokenApply})
	callOpApplyWarmupMetaToken(t, f, "stable-2", []byte{proto.WarmupMetaTokenApply})

	f.currentWarmUpWorkerMutex.RLock()
	countBefore := len(f.currentWarmUpWorkers)
	f.currentWarmUpWorkerMutex.RUnlock()

	for i := 0; i < 10; i++ {
		callOpApplyWarmupMetaToken(t, f, "stable-1", []byte{proto.WarmupMetaTokenRenew})
		callOpApplyWarmupMetaToken(t, f, "stable-2", []byte{proto.WarmupMetaTokenRenew})
	}

	f.currentWarmUpWorkerMutex.RLock()
	countAfter := len(f.currentWarmUpWorkers)
	f.currentWarmUpWorkerMutex.RUnlock()
	require.Equal(t, countBefore, countAfter)
}
