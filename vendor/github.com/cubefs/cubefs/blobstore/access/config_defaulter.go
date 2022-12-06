// Copyright 2022 The CubeFS Authors.
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

package access

const (
	defaultMaxBlobSize uint32 = 1 << 22 // 4MB

	defaultDiskPunishIntervalS    int = 60
	defaultServicePunishIntervalS int = 60
	defaultAllocRetryTimes        int = 3
	defaultAllocRetryIntervalMS   int = 100
	defaultEncoderConcurrency     int = 1000
	defaultMinReadShardsX         int = 1

	defaultAllocatorErrorPercentThreshold  int = 50
	defaultAllocatorMaxConcurrentRequests  int = 10240
	defaultAllocatorRequestVolumeThreshold int = 100
	defaultAllocatorSleepWindow            int = 2 * 1000
	defaultAllocatorTimeout                int = 30 * 1000
	defaultBlobnodeErrorPercentThreshold   int = 80
	defaultBlobnodeMaxConcurrentRequests   int = 102400
	defaultBlobnodeRequestVolumeThreshold  int = 1000
	defaultBlobnodeSleepWindow             int = 5 * 1000
	defaultBlobnodeTimeout                 int = 600 * 1000
)

// ec buffer
// |     blobsize      |      EC15P12      |       EC6P6       |     EC16P20L2     |     EC6P10L2      |       EC3P3       |
// |      1(1 B)       |   55296(54 KiB)   |   24576(24 KiB)   |   77824(76 KiB)   |   36864(36 KiB)   |   12288(12 KiB)   |
// |   2048(2.0 KiB)   |   55296(54 KiB)   |   24576(24 KiB)   |   77824(76 KiB)   |   36864(36 KiB)   |   12288(12 KiB)   |
// |   12288(12 KiB)   |   55296(54 KiB)   |   24576(24 KiB)   |   77824(76 KiB)   |   36864(36 KiB)   |   24576(24 KiB)   |
// |   65536(64 KiB)   |  117990(115 KiB)  |  131076(128 KiB)  |  155648(152 KiB)  |  196614(192 KiB)  |  131076(128 KiB)  |
// |  524288(512 KiB)  |  943731(922 KiB)  | 1048584(1.0 MiB)  | 1245184(1.2 MiB)  | 1572876(1.5 MiB)  | 1048578(1.0 MiB)  |
// | 1048576(1.0 MiB)  | 1887462(1.8 MiB)  | 2097156(2.0 MiB)  | 2490368(2.4 MiB)  | 3145734(3.0 MiB)  | 2097156(2.0 MiB)  |
// | 2097152(2.0 MiB)  | 3774897(3.6 MiB)  | 4194312(4.0 MiB)  | 4980736(4.8 MiB)  | 6291468(6.0 MiB)  | 4194306(4.0 MiB)  |
// | 4194304(4.0 MiB)  | 7549767(7.2 MiB)  | 8388612(8.0 MiB)  | 9961472(9.5 MiB)  | 12582918(12 MiB)  | 8388612(8.0 MiB)  |
// | 8388608(8.0 MiB)  | 15099507(14 MiB)  | 16777224(16 MiB)  | 19922944(19 MiB)  | 25165836(24 MiB)  | 16777218(16 MiB)  |
// | 16777216(16 MiB)  | 30199014(29 MiB)  | 33554436(32 MiB)  | 39845888(38 MiB)  | 50331654(48 MiB)  | 33554436(32 MiB)  |

const (
	_kib = 1 << 10
	_mib = 1 << 20
)

func getDefaultMempoolSize() map[int]int {
	return map[int]int{
		_kib * 2:   -1, // 2KiB for aligned ranged object
		_kib * 16:  -1,
		_kib * 128: -1,
		_kib * 512: -1,
		_mib * 1:   -1,
		_mib * 2:   -1,
		_mib * 4:   -1,
		_mib * 8:   -1,
		_mib * 16:  -1,
		_mib * 32:  -1,
	}
}
