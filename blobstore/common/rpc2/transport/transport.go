// Package smux is a multiplexing library for Golang.
//
// It relies on an underlying connection to provide reliability and ordering, such as TCP or KCP,
// and provides stream-oriented multiplexing over a single channel.
package transport

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Config is used to tune the Smux session
type Config struct {
	// SMUX Protocol version, support 1,2
	Version int

	// Disabled keepalive
	KeepAliveDisabled bool

	// KeepAliveInterval is how often to send a NOP command to the remote
	KeepAliveInterval time.Duration

	// KeepAliveTimeout is how long the session
	// will be closed if no data has arrived
	KeepAliveTimeout time.Duration

	// MaxFrameSize is used to control the maximum
	// frame size to sent to the remote included frame header size
	MaxFrameSize int

	// MaxReceiveBuffer is used to control the maximum
	// number of data in the buffer pool
	MaxReceiveBuffer int

	// MaxStreamBuffer is used to control the maximum
	// number of data per stream
	MaxStreamBuffer int
}

// DefaultConfig is used to return a default configuration
func DefaultConfig() *Config {
	return &Config{
		Version:           1,
		KeepAliveInterval: 10 * time.Second,
		KeepAliveTimeout:  30 * time.Second,
		MaxFrameSize:      1 << 20,
		MaxReceiveBuffer:  32 * (1 << 20),
		MaxStreamBuffer:   4 * (1 << 20),
	}
}

// VerifyConfig is used to verify the sanity of configuration
func VerifyConfig(config *Config) error {
	if !(config.Version == 1 || config.Version == 2) {
		return errors.New("unsupported protocol version")
	}
	if !config.KeepAliveDisabled {
		if config.KeepAliveInterval == 0 {
			return errors.New("keep-alive interval must be positive")
		}
		if config.KeepAliveTimeout < config.KeepAliveInterval {
			return fmt.Errorf("keep-alive timeout must be larger than keep-alive interval")
		}
	}
	if config.MaxFrameSize <= 0 {
		return errors.New("max frame size must be positive")
	}
	if config.MaxFrameSize > 16777215 {
		return errors.New("max frame size must not be larger than 16777215")
	}
	if config.MaxReceiveBuffer <= 0 {
		return errors.New("max receive buffer must be positive")
	}
	if config.MaxStreamBuffer <= 0 {
		return errors.New("max stream buffer must be positive")
	}
	if config.MaxStreamBuffer > config.MaxReceiveBuffer {
		return errors.New("max stream buffer must not be larger than max receive buffer")
	}
	if config.MaxStreamBuffer > math.MaxInt32 {
		return errors.New("max stream buffer cannot be larger than 2147483647")
	}
	return nil
}

// Server is used to initialize a new server-side connection.
func Server(conn Conn, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, false), nil
}

// Client is used to initialize a new client-side connection.
func Client(conn Conn, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, true), nil
}
