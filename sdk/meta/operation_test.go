package meta

import (
	"github.com/cubefs/cubefs/proto"
	"testing"
)

func Test_containsExtent(t *testing.T) {
	type args struct {
		extentKeys []proto.ExtentKey
		ek         proto.ExtentKey
	}

	var eks []proto.ExtentKey
	eks = append(eks, proto.ExtentKey{
		FileOffset:   0,
		PartitionId:  1,
		ExtentId:     1,
		ExtentOffset: 0,
		Size:         1024,
	})

	eks = append(eks, proto.ExtentKey{
		FileOffset:   1024,
		PartitionId:  2,
		ExtentId:     2,
		ExtentOffset: 1024 * 1024,
		Size:         2048,
	})

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "true_equal",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  2,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         1024,
				},
			},
			want: true,
		},
		{
			name: "true_later_start",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   2048,
					PartitionId:  2,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         1024,
				},
			},
			want: true,
		},
		{
			name: "true_early_end",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  2,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         1024,
				},
			},
			want: true,
		},

		{
			name: "false_no_PartitionId",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  9999,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         1024,
				},
			},
			want: false,
		},
		{
			name: "false_no_ExtentId",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  2,
					ExtentId:     9999,
					ExtentOffset: 1024 * 1024,
					Size:         1024,
				},
			},
			want: false,
		},
		{
			name: "false_no_ExtentOffset",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  2,
					ExtentId:     9999,
					ExtentOffset: 1024 * 9999,
					Size:         1024,
				},
			},
			want: false,
		},
		{
			name: "false_early_start",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   0,
					PartitionId:  2,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         3096,
				},
			},
			want: false,
		},
		{
			name: "true_later_end",
			args: args{
				extentKeys: eks,
				ek: proto.ExtentKey{
					FileOffset:   1024,
					PartitionId:  2,
					ExtentId:     2,
					ExtentOffset: 1024 * 1024,
					Size:         4096,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsExtent(tt.args.extentKeys, tt.args.ek); got != tt.want {
				t.Errorf("containsExtent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_iteratePartitions(t *testing.T) {
	mw, err := NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("create meta wrapper failed")
	}
	var (
		choosen *MetaPartition
	)
	var testFunc operatePartitionFunc = func(mp1 *MetaPartition) (bool, int) {
		if mp1.PartitionID%2 == 0 {
			return false, statusFull
		} else {
			choosen = mp1
			return true, statusOK
		}
	}
	count := 0
	for {
		if mw.iteratePartitions(testFunc) {
			if choosen.PartitionID%2 == 0 {
				t.Fatalf("! choose the statusFull mp[%v], try[%v/10]", choosen.PartitionID, count)
			}
		}
		count++
		if count == 10 {
			break
		}
	}
	if err = mw.updateMetaPartitions(); err != nil {
		t.Errorf("updateMetaPartitons failed: %v", err)
	}
}
