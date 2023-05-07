package diskinfo

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestGetPartition(t *testing.T) {
	partition, err := GetMatchParation("/")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	fmt.Println(partition.String())
}

func TestGetIoCounter(t *testing.T) {
	partition, err := GetMatchParation("/")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	counter, err := GetIoCounter(partition)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	data, _ := json.MarshalIndent(counter, "", "  ")
	fmt.Println(string(data))
}

func TestSample(t *testing.T) {
	partition, err := GetMatchParation("/")
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	sample, err := GetDiskIoSample(partition, 20000*time.Millisecond)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	fmt.Printf("ReadCount:\t%v\n", sample.GetReadCount())
	fmt.Printf("ReadFlow:\t%v\n", sample.GetReadFlow())
	fmt.Printf("ReadTotalWaitTime:\t%v\n", sample.GetReadTotalWaitTime())
	fmt.Printf("ReadAvgWaitTime:\t%v\n", sample.GetReadAvgWaitTime())
	fmt.Printf("MergedReadCount:\t%v\n", sample.GetMergedReadCount())

	fmt.Printf("WriteCount:\t%v\n", sample.GetWriteCount())
	fmt.Printf("WriteFlow:\t%v\n", sample.GetWriteFlow())
	fmt.Printf("WriteTotalWaitTime:\t%v\n", sample.GetWriteTotalWaitTime())
	fmt.Printf("WriteAvgWaitTime:\t%v\n", sample.GetWriteAvgWaitTime())
	fmt.Printf("MergedWriteCount:\t%v\n", sample.GetMergedWriteCount())

	fmt.Printf("IoCount:\t%v\n", sample.GetIoCount())
	fmt.Printf("IoTotalWait:\t%v\n", sample.GetIoTotalWaitTime())
	fmt.Printf("IoAvgWaitTime:\t%v\n", sample.GetIoAvgWaitTime())
	fmt.Printf("WeightedIoAvgWaitTime:\t%v\n", sample.GetWeightedAvgWaitTime())
	fmt.Printf("IoInProgress:\t%v\n", sample.GetIopsInProgress())
	fmt.Printf("Sample Duration:\t%v\n", sample.GetSampleDuration())
}
