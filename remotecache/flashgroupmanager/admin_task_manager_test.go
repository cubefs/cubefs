package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/assert"
)

// This file contains test cases for the syncSendAdminTask method of AdminTaskManager
// Tests cover various network scenarios, error conditions, and edge cases
// Uses real TCP connections to simulate network environment, ensuring test authenticity

func TestAdminTaskManager_syncSendAdminTask(t *testing.T) {
	// Test various scenarios of the syncSendAdminTask method
	// Including successful sending, connection failure, task build failure, response errors, etc.
	tests := []struct {
		name        string
		task        *proto.AdminTask
		expectError bool
		setupMock   func() (net.Listener, string)
	}{
		{
			name: "successful_send", // Test successful task sending
			task: &proto.AdminTask{
				ID:           "test_task_1",
				OpCode:       proto.OpVersionOperation,
				OperatorAddr: "127.0.0.1:8080",
				Request:      "test_request",
			},
			expectError: false,
			setupMock: func() (net.Listener, string) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("Failed to create listener: %v", err)
				}

				go func() {
					// Simulate server side: accept connection and process request
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					defer conn.Close()

					// Read data packet sent by client
					packet := proto.NewPacket()
					if err := packet.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime); err != nil {
						return
					}

					// Send success response to client
					responsePacket := proto.NewPacket()
					responsePacket.ResultCode = proto.OpOk
					responsePacket.Data = []byte("success")
					responsePacket.WriteToConn(conn)
				}()

				return listener, listener.Addr().String()
			},
		},
		{
			name: "connection_failed", // Test connection failure: use invalid port to simulate network connection failure
			task: &proto.AdminTask{
				ID:           "test_task_2",
				OpCode:       proto.OpVersionOperation,
				OperatorAddr: "127.0.0.1:9999", // Invalid port
				Request:      "test_request",
			},
			expectError: true,
			setupMock: func() (net.Listener, string) {
				return nil, "127.0.0.1:9999"
			},
		},
		{
			name: "task_build_failed", // Test task build failure: use channel type to cause JSON serialization failure
			task: &proto.AdminTask{
				ID:           "test_task_3",
				OpCode:       proto.OpVersionOperation,
				OperatorAddr: "127.0.0.1:8080",
				Request:      make(chan int), // This will cause JSON marshal to fail
			},
			expectError: true,
			setupMock: func() (net.Listener, string) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("Failed to create listener: %v", err)
				}
				return listener, listener.Addr().String()
			},
		},
		{
			name: "response_error_code", // Test response error code: simulate server returning error response code
			task: &proto.AdminTask{
				ID:           "test_task_4",
				OpCode:       proto.OpVersionOperation,
				OperatorAddr: "127.0.0.1:8080",
				Request:      "test_request",
			},
			expectError: true,
			setupMock: func() (net.Listener, string) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("Failed to create listener: %v", err)
				}

				go func() {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					defer conn.Close()

					// Read the packet
					packet := proto.NewPacket()
					if err := packet.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime); err != nil {
						return
					}

					// Send error response
					responsePacket := proto.NewPacket()
					responsePacket.ResultCode = proto.ErrCodeParamError
					responsePacket.Data = []byte("error")
					responsePacket.WriteToConn(conn)
				}()

				return listener, listener.Addr().String()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up mock network environment for each test case
			var listener net.Listener
			var targetAddr string

			if tt.setupMock != nil {
				listener, targetAddr = tt.setupMock()
				if listener != nil {
					defer listener.Close()
				}
			}

			// Create task manager instance
			manager := newAdminTaskManager(targetAddr, "test_cluster")
			manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
			defer func() {
				close(manager.exitCh)
				time.Sleep(100 * time.Millisecond) // Wait for goroutine to exit
			}()
			// Execute synchronous task sending test
			packet, err := manager.SyncSendAdminTask(tt.task)

			// Verify test results based on expected results
			if tt.expectError {
				assert.Error(t, err) // Expect error to occur
			} else {
				assert.NoError(t, err)                         // Expect no error
				assert.NotNil(t, packet)                       // Expect valid packet to be returned
				assert.Equal(t, proto.OpOk, packet.ResultCode) // Expect operation to succeed
			}
		})
	}
}

func TestAdminTaskManager_syncSendAdminTask_WriteError(t *testing.T) {
	// Test network write error scenario
	// Simulate connection being closed immediately after establishment, causing write failure
	task := &proto.AdminTask{
		ID:           "test_task_write_error",
		OpCode:       proto.OpVersionOperation,
		OperatorAddr: "127.0.0.1:8080",
		Request:      "test_request",
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	targetAddr := listener.Addr().String()

	go func() {
		// Simulate server side: accept connection and close immediately to simulate network write error
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// Close connection immediately to cause write error
		conn.Close()
	}()

	manager := newAdminTaskManager(targetAddr, "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()
	packet, err := manager.SyncSendAdminTask(task)

	assert.Error(t, err)
	assert.Nil(t, packet)
}

func TestAdminTaskManager_syncSendAdminTask_ReadError(t *testing.T) {
	// Test network read error scenario
	// Simulate server sending invalid data, causing read failure
	task := &proto.AdminTask{
		ID:           "test_task_read_error",
		OpCode:       proto.OpVersionOperation,
		OperatorAddr: "127.0.0.1:8080",
		Request:      "test_request",
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	targetAddr := listener.Addr().String()

	go func() {
		// Simulate server side: accept connection and send invalid data to simulate read error
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send invalid data to cause read error
		conn.Write([]byte("invalid_packet_data"))
	}()

	manager := newAdminTaskManager(targetAddr, "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()

	packet, err := manager.SyncSendAdminTask(task)

	assert.Error(t, err)
	assert.Nil(t, packet)
}

func TestAdminTaskManager_syncSendAdminTask_Timeout(t *testing.T) {
	// Test network timeout scenario
	// Simulate server not responding, causing read timeout
	task := &proto.AdminTask{
		ID:           "test_task_timeout",
		OpCode:       proto.OpVersionOperation,
		OperatorAddr: "127.0.0.1:8080",
		Request:      "test_request",
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	targetAddr := listener.Addr().String()

	go func() {
		// Simulate server side: accept connection and don't respond to simulate network timeout
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Don't send any response to cause timeout
		time.Sleep(proto.SyncSendTaskDeadlineTime + time.Second)
	}()

	manager := newAdminTaskManager(targetAddr, "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()

	// Note: useConnPool is a constant, so we can't modify it in tests
	// The test will use the default connection pool behavior

	packet, err := manager.SyncSendAdminTask(task)

	assert.Error(t, err)
	assert.Nil(t, packet)
}

func TestAdminTaskManager_syncSendAdminTask_InvalidTask(t *testing.T) {
	// Test invalid task scenario: pass nil task
	// Verify method's ability to handle abnormal input
	manager := newAdminTaskManager("127.0.0.1:8080", "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()

	// Expected to panic or return error, as nil task cannot be processed
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("task has recover")
		}
	}()

	_, err := manager.SyncSendAdminTask(nil)
	assert.Error(t, err)
}

func TestAdminTaskManager_syncSendAdminTask_EmptyTask(t *testing.T) {
	// Test empty task scenario: all fields are zero values or empty
	// Verify method's ability to handle boundary input
	task := &proto.AdminTask{
		ID:           "",
		OpCode:       0,
		OperatorAddr: "",
		Request:      nil,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	targetAddr := listener.Addr().String()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the packet
		packet := proto.NewPacket()
		if err := packet.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime); err != nil {
			return
		}

		// Send success response
		responsePacket := proto.NewPacket()
		responsePacket.ResultCode = proto.OpOk
		responsePacket.Data = []byte("success")
		responsePacket.WriteToConn(conn)
	}()

	manager := newAdminTaskManager(targetAddr, "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()

	packet, err := manager.SyncSendAdminTask(task)

	assert.NoError(t, err)
	assert.NotNil(t, packet)
	assert.Equal(t, proto.OpOk, packet.ResultCode)
}

func TestAdminTaskManager_syncSendAdminTask_ComplexRequest(t *testing.T) {
	// Test complex request data structure scenario
	// Verify method's ability to handle complex JSON serialization and deserialization
	complexRequest := map[string]interface{}{
		"string_field": "test_value",
		"int_field":    123,
		"bool_field":   true,
		"array_field":  []string{"item1", "item2"},
		"nested_field": map[string]interface{}{
			"nested_key": "nested_value",
		},
	}

	task := &proto.AdminTask{
		ID:           "test_task_complex",
		OpCode:       proto.OpVersionOperation,
		OperatorAddr: "127.0.0.1:8080",
		Request:      complexRequest,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	targetAddr := listener.Addr().String()

	go func() {
		// Simulate server side: accept connection and validate complex data structure
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read data packet sent by client
		packet := proto.NewPacket()
		if err := packet.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime); err != nil {
			return
		}

		// Validate request data integrity: attempt to deserialize complex data structure
		var receivedTask proto.AdminTask
		if err := json.Unmarshal(packet.Data, &receivedTask); err == nil {
			// Data validation successful, send success response
			responsePacket := proto.NewPacket()
			responsePacket.ResultCode = proto.OpOk
			responsePacket.Data = []byte("success")
			responsePacket.WriteToConn(conn)
		}
	}()

	manager := newAdminTaskManager(targetAddr, "test_cluster")
	manager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 5, idleConnTimeout, connectTimeout, false)
	defer func() {
		close(manager.exitCh)
		time.Sleep(100 * time.Millisecond)
	}()

	packet, err := manager.SyncSendAdminTask(task)

	assert.NoError(t, err)
	assert.NotNil(t, packet)
	assert.Equal(t, proto.OpOk, packet.ResultCode)
}
