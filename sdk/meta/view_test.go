// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package meta

import (
	"testing"
	"time"

	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/require"
)

// Documents view.go refresh: first host-latency tick uses RefreshHostLatencyInterval; subsequent Reset uses hostLatencyTimerResetAfterTick.
func TestView_refresh_hostLatencyTimerDurations(t *testing.T) {
	t.Parallel()
	require.Equal(t, 30*time.Second, RefreshHostLatencyInterval)
	require.Equal(t, 10*time.Minute, hostLatencyTimerResetAfterTick())
}

func TestMetaWrapper_refresh_returnsWhenCloseChClosed(t *testing.T) {
	t.Parallel()
	mw := &MetaWrapper{
		closeCh: make(chan struct{}),
		mc:      masterSDK.NewMasterClient([]string{"127.0.0.1:1"}, false),
	}
	close(mw.closeCh)

	done := make(chan struct{})
	go func() {
		mw.refresh()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not exit after closeCh closed")
	}
}
