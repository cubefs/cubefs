// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test"
	"google.golang.org/protobuf/proto"
)

func TestNewGet(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	key := "45"
	keyb := []byte(key)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	get, err := NewGet(ctx, tableb, keyb)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, nil) {
		t.Errorf("Get1 didn't set attributes correctly.")
	}
	get, err = NewGetStr(ctx, table, key)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, nil) {
		t.Errorf("Get2 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Families(fam))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, nil) {
		t.Errorf("Get3 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, nil, filter1) {
		t.Errorf("Get4 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1), Families(fam))
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, filter1) {
		t.Errorf("Get5 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	if err != nil {
		t.Errorf("Get6 didn't set attributes correctly.")
	}
	err = Families(fam)(get)
	if err != nil || !confirmGetAttributes(ctx, get, tableb, keyb, fam, filter1) {
		t.Errorf("Get6 didn't set attributes correctly.")
	}
	_, err = NewGet(ctx, tableb, keyb, MaxVersions(math.MaxInt32))
	if err != nil {
		t.Errorf("Get7 didn't set attributes correctly.")
	}
	_, err = NewGet(ctx, tableb, keyb, MaxVersions(math.MaxInt32+1))
	errStr := "'MaxVersions' exceeds supported number of versions"
	if err != nil && errStr != err.Error() || err == nil {
		t.Errorf("Get8 Expected: %#v\nReceived: %#v", errStr, err)
	}
}

func confirmGetAttributes(ctx context.Context, g *Get, table, key []byte,
	fam map[string][]string, filter1 filter.Filter) bool {
	if g.Context() != ctx ||
		!bytes.Equal(g.Table(), table) ||
		!bytes.Equal(g.Key(), key) ||
		!reflect.DeepEqual(g.families, fam) ||
		(filter1 != nil && g.filter == nil) {
		return false
	}
	return true
}

func TestGetToProto(t *testing.T) {
	var (
		ctx    = context.Background()
		keyStr = "key"
		key    = []byte("key")
		rs     = &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte("region"),
		}
		fil = filter.NewList(filter.MustPassAll, filter.NewKeyOnlyFilter(false))
		fam = map[string][]string{"cookie": []string{"got", "it"}}
	)

	tests := []struct {
		g        *Get
		expProto *pb.GetRequest
	}{
		{
			g: func() *Get {
				get, _ := NewGetStr(ctx, "", keyStr)
				return get
			}(),
			expProto: &pb.GetRequest{
				Region: rs,
				Get: &pb.Get{
					Row:       key,
					Column:    []*pb.Column{},
					TimeRange: &pb.TimeRange{},
				},
			},
		},
		{ // explicitly set configurable attributes to default values
			g: func() *Get {
				get, _ := NewGetStr(ctx, "", keyStr,
					MaxResultsPerColumnFamily(DefaultMaxResultsPerColumnFamily),
					ResultOffset(0),
					MaxVersions(DefaultMaxVersions),
					CacheBlocks(DefaultCacheBlocks),
					TimeRangeUint64(MinTimestamp, MaxTimestamp),
				)
				return get
			}(),
			expProto: &pb.GetRequest{
				Region: rs,
				Get: &pb.Get{
					Row:         key,
					Column:      []*pb.Column{},
					TimeRange:   &pb.TimeRange{},
					StoreLimit:  nil,
					StoreOffset: nil,
					MaxVersions: nil,
					CacheBlocks: nil,
				},
			},
		},
		{ // set configurable options to non-default values
			g: func() *Get {
				get, _ := NewGetStr(ctx, "", keyStr,
					MaxResultsPerColumnFamily(22),
					ResultOffset(7),
					MaxVersions(4),
					CacheBlocks(!DefaultCacheBlocks),
					TimeRangeUint64(3456, 6789),
				)
				return get
			}(),
			expProto: &pb.GetRequest{
				Region: rs,
				Get: &pb.Get{
					Row:    key,
					Column: []*pb.Column{},
					TimeRange: &pb.TimeRange{
						From: proto.Uint64(3456),
						To:   proto.Uint64(6789),
					},
					StoreLimit:  proto.Uint32(22),
					StoreOffset: proto.Uint32(7),
					MaxVersions: proto.Uint32(4),
					CacheBlocks: proto.Bool(!DefaultCacheBlocks),
				},
			},
		},
		{ // set filters, families, and existenceOnly
			g: func() *Get {
				get, _ := NewGetStr(ctx, "", keyStr,
					Filters(fil),
					Families(fam),
				)
				get.ExistsOnly()
				return get
			}(),
			expProto: func() *pb.GetRequest {
				pbFilter, _ := fil.ConstructPBFilter()
				return &pb.GetRequest{
					Region: rs,
					Get: &pb.Get{
						Row:           key,
						Column:        familiesToColumn(fam),
						TimeRange:     &pb.TimeRange{},
						ExistenceOnly: proto.Bool(true),
						Filter:        pbFilter,
					},
				}
			}(),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			tcase.g.SetRegion(mockRegionInfo([]byte("region")))
			p := tcase.g.ToProto()
			out, ok := p.(*pb.GetRequest)
			if !ok {
				t.Fatalf("f")
			}
			if !proto.Equal(out, tcase.expProto) {
				t.Fatalf("expected %+v, got %+v", tcase.expProto, out)
			}
		})

	}
}

func TestNewScan(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	start := "0"
	stop := "100"
	startb := []byte("0")
	stopb := []byte("100")
	scan, err := NewScan(ctx, tableb)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan1 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan2 didn't set attributes correctly.")
	}
	scan, err = NewScanStr(ctx, table)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan3 didn't set attributes correctly.")
	}
	scan, err = NewScanRangeStr(ctx, table, start, stop)
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, nil, nil,
		DefaultNumberOfRows) {
		t.Errorf("Scan4 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb, Families(fam), Filters(filter1))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, startb, stopb, fam, filter1,
		DefaultNumberOfRows) {
		t.Errorf("Scan5 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, Filters(filter1), Families(fam))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, fam, filter1,
		DefaultNumberOfRows) {
		t.Errorf("Scan6 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, NumberOfRows(1))
	if err != nil || !confirmScanAttributes(ctx, scan, tableb, nil, nil, nil, nil, 1) {
		t.Errorf("Scan7 didn't set number of versions correctly")
	}
}

func TestScanToProto(t *testing.T) {
	var (
		ctx = context.Background()
		rs  = &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte("region"),
		}
		startRow = []byte("start")
		stopRow  = []byte("stop")
		fil      = filter.NewKeyOnlyFilter(false)
		fam      = map[string][]string{"cookie": []string{"got", "it"}}
	)

	tests := []struct {
		s        *Scan
		expProto *pb.ScanRequest
	}{
		{
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "")
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(DefaultMaxResultSize),
					Column:        []*pb.Column{},
					TimeRange:     &pb.TimeRange{},
				},
			},
		},
		{ // explicitly set configurable attributes to default values
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "",
					MaxResultSize(DefaultMaxResultSize),
					ScannerID(math.MaxUint64),
					NumberOfRows(DefaultNumberOfRows),
					MaxResultsPerColumnFamily(DefaultMaxResultsPerColumnFamily),
					ResultOffset(0),
					MaxVersions(DefaultMaxVersions),
					CacheBlocks(DefaultCacheBlocks),
					TimeRangeUint64(MinTimestamp, MaxTimestamp),
				)
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
				ScannerId:               nil,
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(DefaultMaxResultSize),
					Column:        []*pb.Column{},
					TimeRange:     &pb.TimeRange{},
					StoreLimit:    nil,
					StoreOffset:   nil,
					MaxVersions:   nil,
					CacheBlocks:   nil,
				},
			},
		},
		{ // set configurable attributes to non-default values
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "",
					MaxResultSize(52),
					NumberOfRows(37),
					MaxResultsPerColumnFamily(13),
					ResultOffset(7),
					MaxVersions(89),
					CacheBlocks(!DefaultCacheBlocks),
					TimeRangeUint64(1024, 1738),
				)
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(37),
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(52),
					Column:        []*pb.Column{},
					TimeRange: &pb.TimeRange{
						From: proto.Uint64(1024),
						To:   proto.Uint64(1738),
					},
					StoreLimit:  proto.Uint32(13),
					StoreOffset: proto.Uint32(7),
					MaxVersions: proto.Uint32(89),
					CacheBlocks: proto.Bool(!DefaultCacheBlocks),
				},
			},
		},
		{ // test that pb.ScanRequest.Scan is nil when scanner id is specificed
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "",
					MaxResultSize(52),
					NumberOfRows(37),
					ScannerID(4444),
					MaxResultsPerColumnFamily(13),
					ResultOffset(7),
					MaxVersions(89),
					CacheBlocks(!DefaultCacheBlocks),
					TimeRangeUint64(1024, 1738),
				)
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(37),
				ScannerId:               proto.Uint64(4444),
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan:                    nil,
			},
		},
		{ // set reversed attribute
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "", Reversed())
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(DefaultMaxResultSize),
					Column:        []*pb.Column{},
					TimeRange:     &pb.TimeRange{},
					Reversed:      proto.Bool(true),
				},
			},
		},
		{ // scan key range
			s: func() *Scan {
				s, _ := NewScanRange(ctx, nil, startRow, stopRow)
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
				CloseScanner:            proto.Bool(false),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(DefaultMaxResultSize),
					Column:        []*pb.Column{},
					TimeRange:     &pb.TimeRange{},
					StartRow:      startRow,
					StopRow:       stopRow,
				},
			},
		},
		{ // set filters and families
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "", Filters(fil), Families(fam))
				return s
			}(),
			expProto: func() *pb.ScanRequest {
				pbFilter, _ := fil.ConstructPBFilter()
				return &pb.ScanRequest{
					Region:                  rs,
					NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
					CloseScanner:            proto.Bool(false),
					ClientHandlesPartials:   proto.Bool(true),
					ClientHandlesHeartbeats: proto.Bool(true),
					Scan: &pb.Scan{
						MaxResultSize: proto.Uint64(DefaultMaxResultSize),
						Column:        familiesToColumn(fam),
						TimeRange:     &pb.TimeRange{},
						Filter:        pbFilter,
					},
				}
			}(),
		},
		{ // close scanner
			s: func() *Scan {
				s, _ := NewScanStr(ctx, "", CloseScanner())
				return s
			}(),
			expProto: &pb.ScanRequest{
				Region:                  rs,
				NumberOfRows:            proto.Uint32(DefaultNumberOfRows),
				CloseScanner:            proto.Bool(true),
				ClientHandlesPartials:   proto.Bool(true),
				ClientHandlesHeartbeats: proto.Bool(true),
				Scan: &pb.Scan{
					MaxResultSize: proto.Uint64(DefaultMaxResultSize),
					Column:        []*pb.Column{},
					TimeRange:     &pb.TimeRange{},
				},
			},
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			tcase.s.SetRegion(mockRegionInfo([]byte("region")))
			p := tcase.s.ToProto()
			out, ok := p.(*pb.ScanRequest)
			if !ok {
				t.Fatalf("f")
			}
			if !proto.Equal(out, tcase.expProto) {
				t.Fatalf("expected %+v, got %+v", tcase.expProto, out)
			}
		})

	}
}

type mockRegionInfo []byte

func (ri mockRegionInfo) Name() []byte {
	return []byte(ri)
}

func (ri mockRegionInfo) IsUnavailable() bool               { return true }
func (ri mockRegionInfo) AvailabilityChan() <-chan struct{} { return nil }
func (ri mockRegionInfo) MarkUnavailable() bool             { return true }
func (ri mockRegionInfo) MarkAvailable()                    {}
func (ri mockRegionInfo) MarkDead()                         {}
func (ri mockRegionInfo) Context() context.Context          { return nil }
func (ri mockRegionInfo) String() string                    { return "" }
func (ri mockRegionInfo) ID() uint64                        { return 0 }
func (ri mockRegionInfo) StartKey() []byte                  { return nil }
func (ri mockRegionInfo) StopKey() []byte                   { return nil }
func (ri mockRegionInfo) Namespace() []byte                 { return nil }
func (ri mockRegionInfo) Table() []byte                     { return nil }
func (ri mockRegionInfo) SetClient(RegionClient)            {}
func (ri mockRegionInfo) Client() RegionClient              { return nil }

type byFamily []*pb.MutationProto_ColumnValue

func (f byFamily) Len() int      { return len(f) }
func (f byFamily) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byFamily) Less(i, j int) bool {
	return bytes.Compare(f[i].Family, f[j].Family) < 0
}

type byQualifier []*pb.MutationProto_ColumnValue_QualifierValue

func (q byQualifier) Len() int      { return len(q) }
func (q byQualifier) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q byQualifier) Less(i, j int) bool {
	return bytes.Compare(q[i].Qualifier, q[j].Qualifier) < 0
}

type bytesSlice [][]byte

func (p bytesSlice) Len() int           { return len(p) }
func (p bytesSlice) Less(i, j int) bool { return bytes.Compare(p[i], p[j]) < 0 }
func (p bytesSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func bytesSlicesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func bytesSlicesLen(a [][]byte) uint32 {
	var l uint32
	for _, b := range a {
		l += uint32(len(b))
	}
	return l
}

func TestMutate(t *testing.T) {
	var (
		ctx      = context.Background()
		tableStr = "table"
		keyStr   = "key"
		table    = []byte("table")
		key      = []byte("key")
		rs       = &pb.RegionSpecifier{
			Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte("region"),
		}
	)

	tests := []struct {
		in              func() (*Mutate, error)
		inStr           func() (*Mutate, error)
		out             *pb.MutateRequest
		cellblocksProto *pb.MutateRequest
		cellblocks      [][]byte
		cellblocksLen   uint32
		err             error
	}{
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, nil)
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        key,
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 key,
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, nil, Durability(SkipWal))
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, nil, Durability(SkipWal))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        key,
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_SKIP_WAL.Enum(),
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 key,
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_SKIP_WAL.Enum(),
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, nil, Durability(DurabilityType(42)))
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, nil, Durability(DurabilityType(42)))
			},
			err: errors.New("invalid durability value"),
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, nil, TTL(time.Second))
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, nil, TTL(time.Second))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Attribute: []*pb.NameBytesPair{
						&pb.NameBytesPair{
							Name:  &attributeNameTTL,
							Value: []byte("\x00\x00\x00\x00\x00\x00\x03\xe8"),
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Attribute: []*pb.NameBytesPair{
						&pb.NameBytesPair{
							Name:  &attributeNameTTL,
							Value: []byte("\x00\x00\x00\x00\x00\x00\x03\xe8"),
						},
					},
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				})
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 35,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1f\x00\x00\x00\x12\x00\x00\x00\x05\x00\x03"),
				[]byte("key"), []byte("\x02"), []byte("cf"), []byte("q"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("value")},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					"cf1": map[string][]byte{
						"q1": []byte("value"),
						"q2": []byte("value"),
					},
					"cf2": map[string][]byte{
						"q1": []byte("value"),
					},
				})
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf1": map[string][]byte{
						"q1": []byte("value"),
						"q2": []byte("value"),
					},
					"cf2": map[string][]byte{
						"q1": []byte("value"),
					},
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf1"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q1"),
									Value:     []byte("value"),
								},
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q2"),
									Value:     []byte("value"),
								},
							},
						},
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf2"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q1"),
									Value:     []byte("value"),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(3),
				},
			},
			cellblocksLen: 111,
			cellblocks: [][]byte{[]byte("\x00\x00\x00!\x00\x00\x00\x14\x00\x00\x00\x05\x00\x03"),
				[]byte("key"), []byte("\x03"), []byte("cf1"), []byte("q1"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("value"),
				[]byte("\x00\x00\x00!\x00\x00\x00\x14\x00\x00\x00\x05\x00\x03"), []byte("key"),
				[]byte("\x03"), []byte("cf1"), []byte("q2"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("value"),
				[]byte("\x00\x00\x00!\x00\x00\x00\x14\x00\x00\x00\x05\x00\x03"), []byte("key"),
				[]byte("\x03"), []byte("cf2"), []byte("q1"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("value")},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, Timestamp(time.Unix(0, 42*1e6)))
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, Timestamp(time.Unix(0, 42*1e6)))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
									Timestamp: proto.Uint64(42),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 35,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1f\x00\x00\x00\x12\x00\x00\x00\x05\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte("q"),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\x04"), []byte("value")},
		},
		{
			in: func() (*Mutate, error) {
				return NewPut(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			inStr: func() (*Mutate, error) {
				return NewPutStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_PUT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("value"),
									Timestamp: proto.Uint64(42),
								},
							},
						},
					},
				},
			},

			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_PUT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 35,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1f\x00\x00\x00\x12\x00\x00\x00\x05\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte("q"),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\x04"), []byte("value")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, nil)
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"q": []byte("value"),
					},
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:  proto.Uint64(42),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte("q"),
									Value:      []byte("value"),
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum(),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 35,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1f\x00\x00\x00\x12\x00\x00\x00\x05\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte("q"),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\f"), []byte("value")},
		},
		{
			in: func() (*Mutate, error) {
				return NewApp(ctx, table, key, nil)
			},
			inStr: func() (*Mutate, error) {
				return NewAppStr(ctx, tableStr, keyStr, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_APPEND.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_APPEND.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewInc(ctx, table, key, nil)
			},
			inStr: func() (*Mutate, error) {
				return NewIncStr(ctx, tableStr, keyStr, nil)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_INCREMENT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_INCREMENT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(0),
				},
			},
		},
		{
			in: func() (*Mutate, error) {
				return NewIncSingle(ctx, table, key, "cf", "q", 1)
			},
			inStr: func() (*Mutate, error) {
				return NewIncStrSingle(ctx, tableStr, keyStr, "cf", "q", 1)
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_INCREMENT.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier: []byte("q"),
									Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_INCREMENT.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 38,
			cellblocks: [][]byte{[]byte("\x00\x00\x00\"\x00\x00\x00\x12\x00\x00\x00\b\x00\x03"),
				[]byte("key"), []byte("\x02"), []byte("cf"), []byte("q"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"),
				[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				})
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": nil,
				})
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									DeleteType: pb.MutationProto_DELETE_FAMILY.Enum(),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 29,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x19\x00\x00\x00\x11\x00\x00\x00\x00\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte(""),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x0e")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42))
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42))
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_FAMILY.Enum(),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 29,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x19\x00\x00\x00\x11\x00\x00\x00\x00\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte(""),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\x0e")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42), DeleteOneVersion())
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": nil,
				}, TimestampUint64(42), DeleteOneVersion())
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte{},
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_FAMILY_VERSION.Enum(),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 29,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x19\x00\x00\x00\x11\x00\x00\x00\x00\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte(""),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\n")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"a": nil,
					},
				}, TimestampUint64(42), DeleteOneVersion())
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, map[string]map[string][]byte{
					"cf": map[string][]byte{
						"a": nil,
					},
				}, TimestampUint64(42), DeleteOneVersion())
			},
			out: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:        []byte(key),
					Timestamp:  proto.Uint64(42),
					MutateType: pb.MutationProto_DELETE.Enum(),
					Durability: pb.MutationProto_USE_DEFAULT.Enum(),
					ColumnValue: []*pb.MutationProto_ColumnValue{
						&pb.MutationProto_ColumnValue{
							Family: []byte("cf"),
							QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
								&pb.MutationProto_ColumnValue_QualifierValue{
									Qualifier:  []byte("a"),
									Timestamp:  proto.Uint64(42),
									DeleteType: pb.MutationProto_DELETE_ONE_VERSION.Enum(),
								},
							},
						},
					},
				},
			},
			cellblocksProto: &pb.MutateRequest{
				Region: rs,
				Mutation: &pb.MutationProto{
					Row:                 []byte(key),
					MutateType:          pb.MutationProto_DELETE.Enum(),
					Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
					Timestamp:           proto.Uint64(42),
					AssociatedCellCount: proto.Int32(1),
				},
			},
			cellblocksLen: 30,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1a\x00\x00\x00\x12\x00\x00\x00\x00\x00\x03"), []byte("key"),
				[]byte("\x02"), []byte("cf"), []byte("a"),
				[]byte("\x00\x00\x00\x00\x00\x00\x00*\b")},
		},
		{
			in: func() (*Mutate, error) {
				return NewDel(ctx, table, key, nil, DeleteOneVersion())
			},
			inStr: func() (*Mutate, error) {
				return NewDelStr(ctx, tableStr, keyStr, nil, DeleteOneVersion())
			},
			err: errors.New(
				"'DeleteOneVersion' option cannot be specified for delete entire row request"),
		},
	}

	run := func(t *testing.T, i int, m *Mutate) {
		t.Helper()
		if m.Name() != "Mutate" {
			t.Fatalf("Expected name to be 'Mutate', got %s", m.Name())
		}

		_, ok := m.NewResponse().(*pb.MutateResponse)
		if !ok {
			t.Fatalf("Expected response to have type 'pb.MutateResponse', got %T",
				m.NewResponse())
		}

		m.SetRegion(mockRegionInfo([]byte("region")))

		// test ToProto
		p := m.ToProto()
		mr, ok := p.(*pb.MutateRequest)
		if !ok {
			t.Fatal("expected proto be of type *pb.MutateRequest")
		}

		sort.Sort(byFamily(mr.Mutation.ColumnValue))
		for _, cv := range mr.Mutation.ColumnValue {
			sort.Sort(byQualifier(cv.QualifierValue))
		}

		tcase := tests[i]

		if !proto.Equal(tcase.out, mr) {
			t.Errorf("expected %v, got %v", tcase.out, mr)
		}

		// test cellblocks
		cellblocksProto, cellblocks, cellblocksLen := m.SerializeCellBlocks()
		mr, ok = cellblocksProto.(*pb.MutateRequest)
		if !ok {
			t.Fatal("expected proto be of type *pb.MutateRequest")
		}

		if !proto.Equal(tcase.cellblocksProto, mr) {
			t.Errorf("expected cellblocks proto %v, got %v",
				tcase.cellblocksProto, cellblocksProto)
		}

		if cellblocksLen != tcase.cellblocksLen {
			t.Errorf("expected cellblocks length %d, got %d", tcase.cellblocksLen, cellblocksLen)
		}

		// check total length matches the returned
		if l := bytesSlicesLen(cellblocks); l != cellblocksLen {
			t.Errorf("total length of cellblocks %d doesn't match returned %d",
				l, cellblocksLen)
		}

		// because maps are iterated in random order, the best we can do here
		// is sort cellblocks and make sure that each byte slice equals. This doesn't
		// test that byte slices are written out in the correct order.
		expected := make([][]byte, len(tcase.cellblocks))
		copy(expected, tcase.cellblocks)
		got := make([][]byte, len(cellblocks))
		copy(got, cellblocks)

		sort.Sort(bytesSlice(expected))
		sort.Sort(bytesSlice(got))
		if !bytesSlicesEqual(expected, got) {
			t.Errorf("expected cellblocks %q, got %q", tcase.cellblocks, cellblocks)
		}
	}
	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m, err := tcase.in()
			if !test.ErrEqual(tcase.err, err) {
				t.Fatalf("expected %v, got %v", tcase.err, err)
			}
			if tcase.err != nil {
				return
			}
			run(t, i, m)
		})
		t.Run(strconv.Itoa(i)+" str", func(t *testing.T) {
			m, err := tcase.inStr()
			if !test.ErrEqual(tcase.err, err) {
				t.Fatalf("expected %v, got %v", tcase.err, err)
			}
			if tcase.err != nil {
				return
			}
			run(t, i, m)
		})
	}
}

var expectedCells = []*pb.Cell{
	&pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("b"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
	},
	&pb.Cell{
		Row:       []byte("row7"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Timestamp: proto.Uint64(1494873081120),
		Value:     []byte("Hello my name is Dog."),
		CellType:  pb.CellType_PUT.Enum(),
	},
}
var cellblock = []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
	102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
	97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

func TestDeserializeCellBlocksGet(t *testing.T) {
	// the first cell is already in protobuf
	getResp := &pb.GetResponse{Result: &pb.Result{
		Cell:                []*pb.Cell{expectedCells[0]},
		AssociatedCellCount: proto.Int32(1),
	}}
	g := &Get{}
	n, err := g.DeserializeCellBlocks(getResp, cellblock)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedCells, getResp.Result.Cell) {
		t.Errorf("expected %v, got %v", expectedCells, getResp.Result.Cell)
	}

	if int(n) != len(cellblock) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	getResp = &pb.GetResponse{Result: &pb.Result{
		AssociatedCellCount: proto.Int32(1),
	}}
	_, err = g.DeserializeCellBlocks(getResp, cellblock[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func TestDeserializeCellblocksMutate(t *testing.T) {
	// the first cell is already in protobuf
	mResp := &pb.MutateResponse{Result: &pb.Result{
		Cell:                []*pb.Cell{expectedCells[0]},
		AssociatedCellCount: proto.Int32(1),
	}}
	m := &Mutate{}
	n, err := m.DeserializeCellBlocks(mResp, cellblock)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(expectedCells, mResp.Result.Cell) {
		t.Errorf("expected %v, got %v", expectedCells, mResp.Result.Cell)
	}

	if int(n) != len(cellblock) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	mResp = &pb.MutateResponse{Result: &pb.Result{
		Cell:                expectedCells[:1],
		AssociatedCellCount: proto.Int32(1),
	}}
	_, err = m.DeserializeCellBlocks(mResp, cellblock[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func TestDeserializeCellBlocksScan(t *testing.T) {
	expectedResults := []*pb.Result{
		&pb.Result{
			Cell: []*pb.Cell{
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("c"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("b"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
			},
			Partial: proto.Bool(true),
		},
		&pb.Result{
			Cell: []*pb.Cell{
				&pb.Cell{
					Row:       []byte("row7"),
					Family:    []byte("cf"),
					Qualifier: []byte("a"),
					Timestamp: proto.Uint64(1494873081120),
					Value:     []byte("Hello my name is Dog."),
					CellType:  pb.CellType_PUT.Enum(),
				},
			},
			Partial: proto.Bool(false),
		},
	}
	cellblocks := []byte{0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 99, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46,
		0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 98, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46,
		0, 0, 0, 48, 0, 0, 0, 19, 0, 0, 0, 21, 0, 4, 114, 111, 119, 55, 2, 99,
		102, 97, 0, 0, 1, 92, 13, 97, 5, 32, 4, 72, 101, 108, 108, 111, 32, 109, 121, 32, 110,
		97, 109, 101, 32, 105, 115, 32, 68, 111, 103, 46}

	scanResp := &pb.ScanResponse{
		Results:              []*pb.Result{},
		PartialFlagPerResult: []bool{true, false},
		CellsPerResult:       []uint32{2, 1},
	}
	s := &Scan{}
	n, err := s.DeserializeCellBlocks(scanResp, cellblocks)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedResults, scanResp.Results) {
		t.Errorf("expected %v, got %v", expectedResults, scanResp.Results)
	}

	if int(n) != len(cellblocks) {
		t.Errorf("expected read %d, got read %d", len(cellblock), n)
	}

	// test error case
	scanResp = &pb.ScanResponse{
		PartialFlagPerResult: []bool{true, false},
		CellsPerResult:       []uint32{2, 1},
	}
	_, err = s.DeserializeCellBlocks(scanResp, cellblocks[:10])
	if err == nil {
		t.Error("expected error, got none")
	}
}

func confirmScanAttributes(ctx context.Context, s *Scan, table, start, stop []byte,
	fam map[string][]string, fltr filter.Filter, numberOfRows uint32) bool {
	if fltr == nil && s.filter != nil {
		return false
	}
	return s.Context() == ctx &&
		bytes.Equal(s.Table(), table) &&
		bytes.Equal(s.StartRow(), start) &&
		bytes.Equal(s.StopRow(), stop) &&
		reflect.DeepEqual(s.families, fam) &&
		s.numberOfRows == numberOfRows
}

func BenchmarkMutateToProtoWithNestedMaps(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := map[string]map[string][]byte{
			"cf": map[string][]byte{
				"a": []byte{10},
				"b": []byte{20},
				"c": []byte{30, 0},
				"d": []byte{40, 0, 0, 0},
				"e": []byte{50, 0, 0, 0, 0, 0, 0, 0},
				"f": []byte{60},
				"g": []byte{70},
				"h": []byte{80, 0},
				"i": []byte{90, 0, 0, 0},
				"j": []byte{100, 0, 0, 0, 0, 0, 0, 0},
				"k": []byte{0, 0, 220, 66},
				"l": []byte{0, 0, 0, 0, 0, 0, 94, 64},
				"m": []byte{0, 0, 2, 67, 0, 0, 0, 0},
				"n": []byte{0, 0, 0, 0, 0, 128, 97, 64, 0, 0, 0, 0, 0, 0, 0, 0},
				"o": []byte{150},
				"p": []byte{4, 8, 15, 26, 23, 42},
				"q": []byte{1, 1, 3, 5, 8, 13, 21, 34, 55},
				"r": []byte("This is a test string."),
			},
		}
		mutate, err := NewPutStr(context.Background(), "", "", data)
		if err != nil {
			b.Errorf("Error creating mutate: %v", err)
		}

		if p := mutate.ToProto(); p == nil {
			b.Fatal("got a nil proto")
		}
	}
}
