// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test"
	"google.golang.org/protobuf/proto"
)

type bytesSlice [][]byte

func (p bytesSlice) Len() int           { return len(p) }
func (p bytesSlice) Less(i, j int) bool { return bytes.Compare(p[i], p[j]) < 0 }
func (p bytesSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type RegionActions []*pb.RegionAction

func (a RegionActions) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a RegionActions) Len() int      { return len(a) }
func (a RegionActions) Less(i, j int) bool {
	return bytes.Compare(a[i].Region.Value, a[j].Region.Value) < 0
}

var (
	reg0 = NewInfo(0, nil, []byte("reg0"),
		[]byte("reg0,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."), nil, nil)
	reg1 = NewInfo(0, nil, []byte("reg1"),
		[]byte("reg1,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."), nil, nil)
	reg2 = NewInfo(0, nil, []byte("reg2"),
		[]byte("reg2,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."), nil, nil)
)

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

func TestMultiToProto(t *testing.T) {
	ctrl := test.NewController(t)
	defer ctrl.Finish()

	values := map[string]map[string][]byte{
		"cf": map[string][]byte{
			"c": []byte("v"),
		},
	}
	valuesProto := []*pb.MutationProto_ColumnValue{
		&pb.MutationProto_ColumnValue{
			Family: []byte("cf"),
			QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
				&pb.MutationProto_ColumnValue_QualifierValue{
					Qualifier: []byte("c"),
					Value:     []byte("v"),
				},
			},
		},
	}
	delValues := map[string]map[string][]byte{
		"cf": map[string][]byte{
			"c": nil,
		},
	}
	delProto := []*pb.MutationProto_ColumnValue{
		&pb.MutationProto_ColumnValue{
			Family: []byte("cf"),
			QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
				&pb.MutationProto_ColumnValue_QualifierValue{
					Qualifier:  []byte("c"),
					DeleteType: pb.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum(),
				},
			},
		},
	}
	appendProto := []*pb.MutationProto_ColumnValue{
		&pb.MutationProto_ColumnValue{
			Family: []byte("cf"),
			QualifierValue: []*pb.MutationProto_ColumnValue_QualifierValue{
				&pb.MutationProto_ColumnValue_QualifierValue{
					Qualifier: []byte("c"),
				},
			},
		},
	}

	tests := []struct {
		calls           []hrpc.Call
		out             *pb.MultiRequest
		panicMsg        string
		cellblocksProto *pb.MultiRequest
		cellblocks      [][]byte
		cellblocksLen   uint32
	}{
		{
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 5)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewPutStr(context.Background(), "reg0", "call1", values)
				cs[1].SetRegion(reg0)
				cs[2], _ = hrpc.NewAppStr(context.Background(), "reg1", "call2", values)
				cs[2].SetRegion(reg1)
				cs[3], _ = hrpc.NewDelStr(context.Background(), "reg1", "call3", delValues)
				cs[3].SetRegion(reg1)
				cs[4], _ = hrpc.NewIncStr(context.Background(), "reg2", "call4", delValues)
				cs[4].SetRegion(reg2)
				return cs
			}(),
			out: &pb.MultiRequest{
				RegionAction: []*pb.RegionAction{
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg0,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(1), Get: &pb.Get{
								Row: []byte("call0"), TimeRange: &pb.TimeRange{}}},
							&pb.Action{Index: proto.Uint32(2), Mutation: &pb.MutationProto{
								Row:         []byte("call1"),
								MutateType:  pb.MutationProto_PUT.Enum(),
								Durability:  pb.MutationProto_USE_DEFAULT.Enum(),
								ColumnValue: valuesProto,
							}},
						},
					},
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg1,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(3), Mutation: &pb.MutationProto{
								Row:         []byte("call2"),
								MutateType:  pb.MutationProto_APPEND.Enum(),
								Durability:  pb.MutationProto_USE_DEFAULT.Enum(),
								ColumnValue: valuesProto,
							}},
							&pb.Action{Index: proto.Uint32(4), Mutation: &pb.MutationProto{
								Row:         []byte("call3"),
								MutateType:  pb.MutationProto_DELETE.Enum(),
								Durability:  pb.MutationProto_USE_DEFAULT.Enum(),
								ColumnValue: delProto,
							}},
						},
					},
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg2,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(5), Mutation: &pb.MutationProto{
								Row:         []byte("call4"),
								MutateType:  pb.MutationProto_INCREMENT.Enum(),
								Durability:  pb.MutationProto_USE_DEFAULT.Enum(),
								ColumnValue: appendProto,
							}},
						},
					},
				},
			},
			cellblocksProto: &pb.MultiRequest{
				RegionAction: []*pb.RegionAction{
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg0,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(1), Get: &pb.Get{
								Row: []byte("call0"), TimeRange: &pb.TimeRange{}}},
							&pb.Action{Index: proto.Uint32(2), Mutation: &pb.MutationProto{
								Row:                 []byte("call1"),
								MutateType:          pb.MutationProto_PUT.Enum(),
								Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
								AssociatedCellCount: proto.Int32(1),
							}},
						},
					},
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg1,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(3), Mutation: &pb.MutationProto{
								Row:                 []byte("call2"),
								MutateType:          pb.MutationProto_APPEND.Enum(),
								Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
								AssociatedCellCount: proto.Int32(1),
							}},
							&pb.Action{Index: proto.Uint32(4), Mutation: &pb.MutationProto{
								Row:                 []byte("call3"),
								MutateType:          pb.MutationProto_DELETE.Enum(),
								Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
								AssociatedCellCount: proto.Int32(1),
							}},
						},
					},
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg2,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(5), Mutation: &pb.MutationProto{
								Row:                 []byte("call4"),
								MutateType:          pb.MutationProto_INCREMENT.Enum(),
								Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
								AssociatedCellCount: proto.Int32(1),
							}},
						},
					},
				},
			},
			cellblocksLen: 130,
			cellblocks: [][]byte{
				[]byte("\x00\x00\x00\x1d\x00\x00\x00\x14\x00\x00\x00\x01\x00\x05"),
				[]byte("call1"), []byte("\x02"), []byte("cf"), []byte("c"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("v"),
				[]byte("\x00\x00\x00\x1d\x00\x00\x00\x14\x00\x00\x00\x01\x00\x05"),
				[]byte("call2"), []byte("\x02"), []byte("cf"), []byte("c"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04"), []byte("v"),
				[]byte("\x00\x00\x00\x1c\x00\x00\x00\x14\x00\x00\x00\x00\x00\x05"),
				[]byte("call3"), []byte("\x02"), []byte("cf"), []byte("c"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\f"),
				[]byte("\x00\x00\x00\x1c\x00\x00\x00\x14\x00\x00\x00\x00\x00\x05"),
				[]byte("call4"), []byte("\x02"), []byte("cf"), []byte("c"),
				[]byte("\u007f\xff\xff\xff\xff\xff\xff\xff\x04")},
		},
		{ // one call with expired context
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 2)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cs[0], _ = hrpc.NewGetStr(ctx, "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg0", "call1", nil)
				cs[1].SetRegion(reg0)
				return cs
			}(),
			out: &pb.MultiRequest{
				RegionAction: []*pb.RegionAction{
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg0,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(2), Mutation: &pb.MutationProto{
								Row:        []byte("call1"),
								MutateType: pb.MutationProto_APPEND.Enum(),
								Durability: pb.MutationProto_USE_DEFAULT.Enum(),
							}},
						},
					},
				},
			},
			cellblocksProto: &pb.MultiRequest{
				RegionAction: []*pb.RegionAction{
					&pb.RegionAction{
						Region: &pb.RegionSpecifier{
							Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
							Value: []byte("reg0,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
						},
						Action: []*pb.Action{
							&pb.Action{Index: proto.Uint32(2), Mutation: &pb.MutationProto{
								Row:                 []byte("call1"),
								MutateType:          pb.MutationProto_APPEND.Enum(),
								Durability:          pb.MutationProto_USE_DEFAULT.Enum(),
								AssociatedCellCount: proto.Int32(0),
							}},
						},
					},
				},
			},
		},
		{ // one batched call is not supported for batching
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 2)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "yolo")
				cs[0].SetRegion(reg0)

				cs[1] = hrpc.NewCreateTable(context.Background(), []byte("yolo"), nil)
				return cs
			}(),
			panicMsg: "unsupported call type for Multi: *hrpc.CreateTable",
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m := newMulti(1000)

			for _, c := range tcase.calls {
				if m.add(c) {
					t.Fatal("multi is full")
				}
			}

			if tcase.panicMsg != "" {
				defer func() {
					r := recover()
					msg, ok := r.(string)
					if r == nil || !ok || msg != tcase.panicMsg {
						t.Errorf("expected panic with %q, got %v", tcase.panicMsg, r)
					}
				}()
				_ = m.ToProto()
				return
			}

			// test proto
			p := m.ToProto()

			out, ok := p.(*pb.MultiRequest)
			if !ok {
				t.Fatalf("unexpected proto type %T", p)
			}

			// check that we recorded correct ordering RegionActions
			if exp, got := len(m.regions), len(out.RegionAction); exp != got {
				t.Fatalf("expected regions length %d, got %d", exp, got)
			}
			for i, r := range m.regions {
				if exp, got := r.Name(), out.RegionAction[i].Region.Value; !bytes.Equal(exp, got) {
					t.Fatalf("regions are different at index %d: %q != %q", i, exp, got)
				}
			}

			// compare that the MultiRequest are as expected
			sort.Sort(RegionActions(tcase.out.RegionAction))
			sort.Sort(RegionActions(out.RegionAction))

			if !proto.Equal(tcase.out, out) {
				t.Fatalf("expected %v, got %v", tcase.out, out)
			}

			// test cellblocks
			m = newMulti(1000)
			for _, c := range tcase.calls {
				if m.add(c) {
					t.Fatal("multi is full")
				}
			}
			cellblocksProto, cellblocks, cellblocksLen := m.SerializeCellBlocks()
			out, ok = cellblocksProto.(*pb.MultiRequest)
			if !ok {
				t.Fatalf("unexpected proto type %T", cellblocksProto)
			}

			sort.Sort(RegionActions(tcase.cellblocksProto.RegionAction))
			sort.Sort(RegionActions(out.RegionAction))

			if !proto.Equal(tcase.cellblocksProto, out) {
				t.Errorf("expected cellblocks proto %v, got %v",
					tcase.cellblocksProto, cellblocksProto)
			}

			if cellblocksLen != tcase.cellblocksLen {
				t.Errorf("expected cellblocks length %d, got %d",
					tcase.cellblocksLen, cellblocksLen)
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
		})
	}
}

func TestMultiReturnResults(t *testing.T) {
	tests := []struct {
		calls       []hrpc.Call
		regions     []hrpc.RegionInfo
		response    proto.Message
		err         error
		shouldPanic bool
		out         []hrpc.RPCResult
	}{
		{ // all good
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 5)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg1", "call1", nil)
				cs[1].SetRegion(reg1)
				cs[2], _ = hrpc.NewAppStr(context.Background(), "reg0", "call2", nil)
				cs[2].SetRegion(reg0)
				cs[3], _ = hrpc.NewGetStr(context.Background(), "reg2", "call3")
				cs[3].SetRegion(reg2)
				cs[4], _ = hrpc.NewAppStr(context.Background(), "reg1", "call4", nil)
				cs[4].SetRegion(reg1)
				return cs
			}(),
			regions: []hrpc.RegionInfo{reg2, reg0, reg1},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg2
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(4),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call3")}},
								},
							},
						},
					},
					// reg0
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(1),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
								},
							},
							&pb.ResultOrException{
								Index: proto.Uint32(3),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call2")}},
								},
							},
						},
					},
					// reg1, results are returned in different order
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(5),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call4")}},
								},
							},
							&pb.ResultOrException{
								Index: proto.Uint32(2),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call1")}},
								},
							},
						},
					},
				},
			},
			out: []hrpc.RPCResult{
				hrpc.RPCResult{Msg: &pb.GetResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
				}}},
				hrpc.RPCResult{Msg: &pb.MutateResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call1")}},
				}}},
				hrpc.RPCResult{Msg: &pb.MutateResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call2")}},
				}}},
				hrpc.RPCResult{Msg: &pb.GetResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call3")}},
				}}},
				hrpc.RPCResult{Msg: &pb.MutateResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call4")}},
				}}},
			},
		},
		{ // a region exception
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 4)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg1", "call1", nil)
				cs[1].SetRegion(reg1)
				cs[2], _ = hrpc.NewAppStr(context.Background(), "reg0", "call2", nil)
				cs[2].SetRegion(reg0)
				cs[3], _ = hrpc.NewAppStr(context.Background(), "reg1", "call3", nil)
				cs[3].SetRegion(reg1)
				return cs
			}(),
			regions: []hrpc.RegionInfo{reg1, reg0},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg1
					&pb.RegionActionResult{
						Exception: &pb.NameBytesPair{ // retryable exception
							Name: proto.String(
								"org.apache.hadoop.hbase.NotServingRegionException"),
							Value: []byte("YOLO"),
						},
					},
					// reg0, results different order
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(3),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call2")}},
								},
							},
							&pb.ResultOrException{
								Index: proto.Uint32(1),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
								},
							},
						},
					},
				},
			},
			out: []hrpc.RPCResult{
				hrpc.RPCResult{Msg: &pb.GetResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
				}}},
				hrpc.RPCResult{Error: NotServingRegionError{errors.New("HBase Java " +
					"exception org.apache.hadoop.hbase.NotServingRegionException:\nYOLO")}},
				hrpc.RPCResult{Msg: &pb.MutateResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call2")}},
				}}},
				hrpc.RPCResult{Error: NotServingRegionError{errors.New("HBase Java " +
					"exception org.apache.hadoop.hbase.NotServingRegionException:\nYOLO")}},
			},
		},
		{ // a result exception
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 2)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg0", "call1", nil)
				cs[1].SetRegion(reg0)
				return cs
			}(),
			regions: []hrpc.RegionInfo{reg0},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(1),
								Exception: &pb.NameBytesPair{
									Name:  proto.String("YOLO"),
									Value: []byte("SWAG"),
								},
							},
							&pb.ResultOrException{
								Index: proto.Uint32(2),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call1")}},
								},
							},
						},
					},
				},
			},
			out: []hrpc.RPCResult{
				hrpc.RPCResult{Error: errors.New("HBase Java exception YOLO:\nSWAG")},
				hrpc.RPCResult{Msg: &pb.MutateResponse{Result: &pb.Result{
					Cell: []*pb.Cell{&pb.Cell{Row: []byte("call1")}},
				}}},
			},
		},
		{ // an unrecoverable error
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 2)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg1", "call1", nil)
				cs[1].SetRegion(reg1)
				return cs
			}(),
			err: ServerError{errors.New("OOOPS")},
			out: []hrpc.RPCResult{
				hrpc.RPCResult{Error: ServerError{errors.New("OOOPS")}},
				hrpc.RPCResult{Error: ServerError{errors.New("OOOPS")}},
			},
		},
		{ // non-MultiResponse
			shouldPanic: true,
			response:    &pb.CreateTableResponse{},
		},
		{ // non-Get or non-Mutate request
			shouldPanic: true,
			calls: []hrpc.Call{
				hrpc.NewCreateTable(context.Background(), []byte("reg0"), nil),
			},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(1),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
								},
							},
						},
					},
				},
			},
		},
		{ // result index is 0
			shouldPanic: true,
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 1)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				return cs
			}(),
			regions: []hrpc.RegionInfo{reg0},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(0),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
								},
							},
						},
					},
				},
			},
		},
		{ // result index is out of bounds
			shouldPanic: true,
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 1)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				return cs
			}(),
			regions: []hrpc.RegionInfo{reg0},
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index: proto.Uint32(2),
								Result: &pb.Result{
									Cell: []*pb.Cell{&pb.Cell{Row: []byte("call0")}},
								},
							},
						},
					},
				},
			},
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m := newMulti(1000)

			for _, c := range tcase.calls {
				if m.add(c) {
					t.Fatal("multi is full")
				}
			}
			m.regions = tcase.regions

			if tcase.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("returnResults should have panicked")
					}
				}()
				m.returnResults(tcase.response, tcase.err)
				return
			}

			m.returnResults(tcase.response, tcase.err)

			for i, c := range tcase.calls {
				expected, out := tcase.out[i], <-c.ResultChan()
				if !test.ErrEqual(expected.Error, out.Error) {
					t.Errorf("expected %v, got %v", expected.Error, out.Error)
				}
				if !proto.Equal(expected.Msg, out.Msg) {
					t.Errorf("expected %v, got %v", expected.Msg, out.Msg)
				}
			}
		})
	}
}

func TestMultiDeserializeCellBlocks(t *testing.T) {
	// TODO: add tests for panics

	getCellblock := "\x00\x00\x00\x1d\x00\x00\x00\x14\x00\x00\x00\x01\x00\x05call0" +
		"\x02cfa\x00\x00\x01]=\xef\x95\xd4\x04*"
	getCell := &pb.Cell{
		Row:       []byte("call0"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Value:     []byte{42},
		Timestamp: proto.Uint64(1499982697940),
		CellType:  pb.CellType(pb.CellType_PUT).Enum(),
	}

	appendCellblock := "\x00\x00\x00\x1e\x00\x00\x00\x14\x00\x00\x00\x02\x00\x05call1" +
		"\x02cfa\x00\x00\x01]=\xef\x95\xec\x04**"
	appendCell := &pb.Cell{
		Row:       []byte("call1"),
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Value:     []byte{42, 42},
		Timestamp: proto.Uint64(1499982697964),
		CellType:  pb.CellType(pb.CellType_PUT).Enum(),
	}

	tests := []struct {
		calls      []hrpc.Call
		response   proto.Message
		cellblocks []byte
		out        *pb.MultiResponse
		err        error
	}{
		{ // all good
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 3)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg1", "call1", nil)
				cs[1].SetRegion(reg1)
				cs[2], _ = hrpc.NewDelStr(context.Background(), "reg1", "call2", nil)
				cs[2].SetRegion(reg1)
				return cs
			}(),
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg1
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Append
								Index: proto.Uint32(2),
								Result: &pb.Result{
									AssociatedCellCount: proto.Int32(1),
								},
							},
							&pb.ResultOrException{ // Delete
								Index: proto.Uint32(3),
								Result: &pb.Result{
									AssociatedCellCount: proto.Int32(0),
								},
							},
						},
					},
					// reg0
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Get
								Index: proto.Uint32(1),
								Result: &pb.Result{
									AssociatedCellCount: proto.Int32(1),
								},
							},
						},
					},
				},
			},
			cellblocks: []byte(appendCellblock + getCellblock),
			out: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg1
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Append
								Index: proto.Uint32(2),
								Result: &pb.Result{
									Cell:                []*pb.Cell{appendCell},
									AssociatedCellCount: proto.Int32(1),
								},
							},
							&pb.ResultOrException{ // Delete
								Index: proto.Uint32(3),
								Result: &pb.Result{
									AssociatedCellCount: proto.Int32(0),
								},
							},
						},
					},
					// reg0
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Get
								Index: proto.Uint32(1),
								Result: &pb.Result{
									Cell:                []*pb.Cell{getCell},
									AssociatedCellCount: proto.Int32(1),
								},
							},
						},
					},
				},
			},
		},
		{ // region exception, a result exception and an ok result
			calls: func() []hrpc.Call {
				cs := make([]hrpc.Call, 3)
				cs[0], _ = hrpc.NewGetStr(context.Background(), "reg0", "call0")
				cs[0].SetRegion(reg0)
				cs[1], _ = hrpc.NewAppStr(context.Background(), "reg1", "call1", nil)
				cs[1].SetRegion(reg1)
				cs[2], _ = hrpc.NewDelStr(context.Background(), "reg1", "call2", nil)
				cs[2].SetRegion(reg1)
				return cs
			}(),
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg1
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Append
								Index: proto.Uint32(2),
								Result: &pb.Result{
									AssociatedCellCount: proto.Int32(1),
								},
							},
							&pb.ResultOrException{ // Delete
								Index: proto.Uint32(3),
								Exception: &pb.NameBytesPair{
									Name: proto.String("YOLO"), Value: []byte("SWAG")},
							},
						},
					},
					// reg0
					&pb.RegionActionResult{
						Exception: &pb.NameBytesPair{
							Name: proto.String("YOLO"), Value: []byte("SWAG")},
					},
				},
			},
			cellblocks: []byte(appendCellblock),
			out: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					// reg1
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{ // Append
								Index: proto.Uint32(2),
								Result: &pb.Result{
									Cell:                []*pb.Cell{appendCell},
									AssociatedCellCount: proto.Int32(1),
								},
							},
							&pb.ResultOrException{ // Delete
								Index: proto.Uint32(3),
								Exception: &pb.NameBytesPair{
									Name: proto.String("YOLO"), Value: []byte("SWAG")},
							},
						},
					},
					// reg0
					&pb.RegionActionResult{
						Exception: &pb.NameBytesPair{
							Name: proto.String("YOLO"), Value: []byte("SWAG")},
					},
				},
			},
		},
		{ // region exception and region results
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						Exception: &pb.NameBytesPair{
							Name: proto.String("YOLO"), Value: []byte("SWAG")},
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index:  proto.Uint32(2),
								Result: &pb.Result{AssociatedCellCount: proto.Int32(1)},
							},
						},
					},
				},
			},
			err: errors.New(
				"got exception for region, but still have 1 result(s) returned from it"),
		},
		{ // no result index
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Result: &pb.Result{AssociatedCellCount: proto.Int32(1)},
							},
						},
					},
				},
			},
			err: errors.New("no index for result in multi response"),
		},
		{ // no result and no exception
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{Index: proto.Uint32(2)},
						},
					},
				},
			},
			err: errors.New("no result or exception for action in multi response"),
		},
		{ // result and exception
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index:  proto.Uint32(2),
								Result: &pb.Result{AssociatedCellCount: proto.Int32(1)},
								Exception: &pb.NameBytesPair{
									Name: proto.String("YOLO"), Value: []byte("SWAG")},
							},
						},
					},
				},
			},
			err: errors.New("got result and exception for action in multi response"),
		},
		{ // single call deserialize error
			calls: func() []hrpc.Call {
				c, _ := hrpc.NewGetStr(context.Background(), "reg0", "call0")
				return []hrpc.Call{callWithCellBlocksError{c}}
			}(),
			response: &pb.MultiResponse{
				RegionActionResult: []*pb.RegionActionResult{
					&pb.RegionActionResult{
						ResultOrException: []*pb.ResultOrException{
							&pb.ResultOrException{
								Index:  proto.Uint32(1),
								Result: &pb.Result{AssociatedCellCount: proto.Int32(1)},
							},
						},
					},
				},
			},
			err: errors.New(
				"error deserializing cellblocks for \"Get\" call as part of MultiResponse: OOPS"),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m := newMulti(1000)

			for _, c := range tcase.calls {
				if m.add(c) {
					t.Fatal("multi is full")
				}
			}

			n, err := m.DeserializeCellBlocks(tcase.response, tcase.cellblocks)
			if l := len(tcase.cellblocks); int(n) != l {
				t.Errorf("expected read %d, got read %d", l, n)
			}

			if !test.ErrEqual(tcase.err, err) {
				t.Fatalf("expected %v, got %v", tcase.err, err)
			}

			if tcase.err != nil {
				return
			}

			if !proto.Equal(tcase.out, tcase.response) {
				t.Fatalf("expected %v, got %v", tcase.out, tcase.response)
			}
		})
	}
}
