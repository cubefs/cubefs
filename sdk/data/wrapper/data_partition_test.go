// Copyright 2020 The Chubao Authors.
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

package wrapper

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestKmin(t *testing.T) {
	partitions := make([]*DataPartition, 0)

	rand.Seed(time.Now().UnixNano())
	length := rand.Intn(100) + 2

	for i := 0; i < length; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Int63n(100)

		dp := new(DataPartition)
		dp.Metrics = new(DataPartitionMetrics)
		dp.Metrics.AvgWriteLatencyNano = i
		partitions = append(partitions, dp)
	}
	fmt.Printf("%-20s", "origin partitions:")
	for _, v := range partitions {
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	kth := selectKminDataPartition(partitions, (length-1)*80/100+1)

	kmin := partitions[kth].GetAvgWrite()

	fmt.Printf("%-20s%v/%v", "kth of length:", kth, length)
	fmt.Println()

	fmt.Printf("%-20s%v", "kmin:", kmin)
	fmt.Println()

	fmt.Printf("%-20s", "faster partitions:")
	for _, v := range partitions[:kth] {
		if v.GetAvgWrite() > kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	fmt.Printf("%-20s", "slower partitions:")
	for _, v := range partitions[kth:] {
		fmt.Printf("%v ", v.GetAvgWrite())
		if v.GetAvgWrite() < kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
	}
	fmt.Println()
}
