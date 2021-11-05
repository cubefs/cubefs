package mock

import (
	"testing"
)

// Quorum 0 and all host success.
func TestReplProtocol_Replica3_Quorum0_1(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 0,
		Hosts: map[int]int{
			1: 10000,
			2: 10001,
			3: 10002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        3,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 0, leader success, 1/2 follower failure.
func TestReplProtocol_Replica3_Quorum0_2(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 0,
		Hosts: map[int]int{
			1: 11000,
			2: 11001,
			3: 11002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Failure,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Failure, // Failure
				},
			},
			{
				ReqID:        3,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 0, leader failure, 2/2 follower success.
func TestReplProtocol_Replica3_Quorum0_3(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 0,
		Hosts: map[int]int{
			1: 12000,
			2: 12001,
			3: 12002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Failure,
				HostResults: map[int]MockResult{
					1: MockResult_Failure,
					2: MockResult_Success,
					3: MockResult_Success, // Failure
				},
			},
			{
				ReqID:        3,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 2, 3/3 success
func TestReplProtocol_Replica3_Quorum2_1(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 2,
		Hosts: map[int]int{
			1: 13000,
			2: 13001,
			3: 13002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        3,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 2, leader success, 1/2 follower success
func TestReplProtocol_Replica3_Quorum2_2(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 2,
		Hosts: map[int]int{
			1: 14000,
			2: 14001,
			3: 14002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Failure, // Failure
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 2, leader failure, 2/2 follower success
func TestReplProtocol_Replica3_Quorum2_3(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 2,
		Hosts: map[int]int{
			1: 15000,
			2: 15001,
			3: 15002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Failure,
				HostResults: map[int]MockResult{
					1: MockResult_Failure, // Failure
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
		},
	}
	RunMockTest(config, t)
}

// Quorum 2, leader success, 2/2 follower failure
func TestReplProtocol_Replica3_Quorum2_4(t *testing.T) {
	var config = &MockTestConfig{
		Quorum: 2,
		Hosts: map[int]int{
			1: 16000,
			2: 16001,
			3: 16002,
		},
		Messages: []MockMessageConfig{
			{
				ReqID:        1,
				ExpectResult: MockResult_Success,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Success,
					3: MockResult_Success,
				},
			},
			{
				ReqID:        2,
				ExpectResult: MockResult_Failure,
				HostResults: map[int]MockResult{
					1: MockResult_Success,
					2: MockResult_Failure, // Failure
					3: MockResult_Failure, // Failure
				},
			},
		},
	}
	RunMockTest(config, t)
}
