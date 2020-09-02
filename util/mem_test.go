package util

import (
	"testing"
)

func TestGetDiskInfo(t *testing.T) {
	info, used, err := GetDiskInfo("./")

	if err != nil {
		t.Fatal(err)
	}

	if info <= 0 || used <= 0 {
		t.Fatal("get info has err")
	}
}

func TestSelectDisk(t *testing.T) {
	var max *diskScore

	result := []*diskScore{
		{
			path:         "1",
			freeMemory:   40 * GB,
			partitionNum: 40,
		},
		{
			path:         "2",
			freeMemory:   10 * GB,
			partitionNum: 10,
		},
		{
			path:         "3",
			freeMemory:   50 * GB,
			partitionNum: 100,
		},
		{
			path:         "4",
			freeMemory:   10 * GB,
			partitionNum: 40,
		},
		{
			path:         "5",
			freeMemory:   0 * GB,
			partitionNum: 40,
		},
		{
			path:         "6",
			freeMemory:   20 * GB,
			partitionNum: 0,
		},
	}

	var (
		sumMemory uint64
		sumCount  int
	)
	for _, ds := range result {
		sumMemory += ds.freeMemory
		sumCount += ds.partitionNum
	}

	for _, ds := range result {
		ds.computeScore(sumMemory, sumCount)
		if max == nil {
			max = ds
		}

		if max.score < ds.score {
			max = ds
		}

		println(ds.path, "score", ds.score)
	}

	if max == nil {
		panic("impossibility")
	}

	println("result path", max.path)

}
