// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"testing"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/region"
)

func TestRegionDiscovery(t *testing.T) {
	client := newClient("~invalid.quorum~") // We shouldn't connect to ZK.

	reg := client.getRegionFromCache([]byte("test"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though the cache was empty?!", reg)
	}

	// Inject a "test" table with a single region that covers the entire key
	// space (both the start and stop keys are empty).
	family := []byte("info")
	metaRow := &hrpc.Result{Cells: []*hrpc.Cell{
		&hrpc.Cell{
			Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
			Family:    family,
			Qualifier: []byte("regioninfo"),
			Value: []byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04test" +
				"\x1a\x00\"\x00(\x000\x008\x00"),
		},
		&hrpc.Cell{
			Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
			Family:    family,
			Qualifier: []byte("seqnumDuringOpen"),
			Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		},
		&hrpc.Cell{
			Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
			Family:    family,
			Qualifier: []byte("server"),
			Value:     []byte("localhost:50966"),
		},
		&hrpc.Cell{
			Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
			Family:    family,
			Qualifier: []byte("serverstartcode"),
			Value:     []byte("\x00\x00\x01N\x02\x92R\xb1"),
		},
	}}

	reg, _, err := region.ParseRegionInfo(metaRow)
	if err != nil {
		t.Fatalf("Failed to discover region: %s", err)
	}
	client.regions.put(reg)

	reg = client.getRegionFromCache([]byte("test"), []byte("theKey"))
	if reg == nil {
		t.Fatal("Region not found even though we injected it in the cache.")
	}
	expected := region.NewInfo(
		0,
		nil,
		[]byte("test"),
		[]byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		nil,
		nil,
	)
	if !bytes.Equal(reg.Table(), expected.Table()) ||
		!bytes.Equal(reg.Name(), expected.Name()) ||
		!bytes.Equal(reg.StartKey(), expected.StartKey()) ||
		!bytes.Equal(reg.StopKey(), expected.StopKey()) {
		t.Errorf("Found region %#v \nbut expected %#v", reg, expected)
	}

	reg = client.getRegionFromCache([]byte("notfound"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though this table doesn't exist", reg)
	}
}
