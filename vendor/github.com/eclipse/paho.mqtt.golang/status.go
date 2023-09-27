/*
 * Copyright (c) 2021 IBM Corp and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 *    Matt Brittan
 */

package mqtt

import (
	"errors"
	"sync"
)

// Status - Manage the connection status

// Multiple go routines will want to access/set this. Previously status was implemented as a `uint32` and updated
// with a mixture of atomic functions and a mutex (leading to some deadlock type issues that were very hard to debug).

// In this new implementation `connectionStatus` takes over managing the state and provides functions that allow the
// client to request a move to a particular state (it may reject these requests!). In some cases the 'state' is
// transitory, for example `connecting`, in those cases a function will be returned that allows the client to move
// to a more static state (`disconnected` or `connected`).

// This "belts-and-braces" may be a little over the top but issues with the status have caused a number of difficult
// to trace bugs in the past and the likelihood that introducing a new system would introduce bugs seemed high!
// I have written this in a way that should make it very difficult to misuse it (but it does make things a little
// complex with functions returning functions that return functions!).

type status uint32

const (
	disconnected  status = iota // default (nil) status is disconnected
	disconnecting               // Transitioning from one of the below states back to disconnected
	connecting
	reconnecting
	connected
)

// String simplify output of statuses
func (s status) String() string {
	switch s {
	case disconnected:
		return "disconnected"
	case disconnecting:
		return "disconnecting"
	case connecting:
		return "connecting"
	case reconnecting:
		return "reconnecting"
	case connected:
		return "connected"
	default:
		return "invalid"
	}
}

type connCompletedFn func(success bool) error
type disconnectCompletedFn func()
type connectionLostHandledFn func(bool) (connCompletedFn, error)

/* State transitions

static states are `disconnected` and `connected`. For all other states a process will hold a function that will move
the state to one of those. That function effectively owns the state and any other changes must not proceed until it
completes. One exception to that is that the state can always be moved to `disconnecting` which provides a signal that
transitions to `connected` will be rejected (this is required because a Disconnect can be requested while in the
Connecting state).

# Basic Operations

The standard workflows are:

disconnected -> `Connecting()` -> connecting -> `connCompletedFn(true)` -> connected
connected -> `Disconnecting()` -> disconnecting -> `disconnectCompletedFn()` -> disconnected
connected -> `ConnectionLost(false)` -> disconnecting -> `connectionLostHandledFn(true/false)` -> disconnected
connected -> `ConnectionLost(true)` -> disconnecting -> `connectionLostHandledFn(true)` -> connected

Unfortunately the above workflows are complicated by the fact that `Disconnecting()` or `ConnectionLost()` may,
potentially, be called at any time (i.e. whilst in the middle of transitioning between states). If this happens:

* The state will be set to disconnecting (which will prevent any request to move the status to connected)
* The call to `Disconnecting()`/`ConnectionLost()` will block until the previously active call completes and then
  handle the disconnection.

Reading the tests (unit_status_test.go) might help understand these rules.
*/

var (
	errAbortConnection                = errors.New("disconnect called whist connection attempt in progress")
	errAlreadyConnectedOrReconnecting = errors.New("status is already connected or reconnecting")
	errStatusMustBeDisconnected       = errors.New("status can only transition to connecting from disconnected")
	errAlreadyDisconnected            = errors.New("status is already disconnected")
	errDisconnectionRequested         = errors.New("disconnection was requested whilst the action was in progress")
	errDisconnectionInProgress        = errors.New("disconnection already in progress")
	errAlreadyHandlingConnectionLoss  = errors.New("status is already Connection Lost")
	errConnLossWhileDisconnecting     = errors.New("connection status is disconnecting so loss of connection is expected")
)

// connectionStatus encapsulates, and protects, the connection status.
type connectionStatus struct {
	sync.RWMutex  // Protects the variables below
	status        status
	willReconnect bool // only used when status == disconnecting. Indicates that an attempt will be made to reconnect (allows us to abort that)

	// Some statuses are transitional (e.g. connecting, connectionLost, reconnecting, disconnecting), that is, whatever
	// process moves us into that status will move us out of it when an action is complete. Sometimes other users
	// will need to know when the action is complete (e.g. the user calls `Disconnect()` whilst the status is
	// `connecting`). `actionCompleted` will be set whenever we move into one of the above statues and the channel
	// returned to anything else requesting a status change. The channel will be closed when the operation is complete.
	actionCompleted chan struct{} // Only valid whilst status is Connecting or Reconnecting; will be closed when connection completed (success or failure)
}

// ConnectionStatus returns the connection status.
// WARNING: the status may change at any time so users should not assume they are the only goroutine touching this
func (c *connectionStatus) ConnectionStatus() status {
	c.RLock()
	defer c.RUnlock()
	return c.status
}

// ConnectionStatusRetry returns the connection status and retry flag (indicates that we expect to reconnect).
// WARNING: the status may change at any time so users should not assume they are the only goroutine touching this
func (c *connectionStatus) ConnectionStatusRetry() (status, bool) {
	c.RLock()
	defer c.RUnlock()
	return c.status, c.willReconnect
}

// Connecting - Changes the status to connecting if that is a permitted operation
// Will do nothing unless the current status is disconnected
// Returns a function that MUST be called when the operation is complete (pass in true if successful)
func (c *connectionStatus) Connecting() (connCompletedFn, error) {
	c.Lock()
	defer c.Unlock()
	// Calling Connect when already connecting (or if reconnecting) may not always be considered an error
	if c.status == connected || c.status == reconnecting {
		return nil, errAlreadyConnectedOrReconnecting
	}
	if c.status != disconnected {
		return nil, errStatusMustBeDisconnected
	}
	c.status = connecting
	c.actionCompleted = make(chan struct{})
	return c.connected, nil
}

// connected is an internal function (it is returned by functions that set the status to connecting or reconnecting,
// calling it completes the operation). `success` is used to indicate whether the operation was successfully completed.
func (c *connectionStatus) connected(success bool) error {
	c.Lock()
	defer func() {
		close(c.actionCompleted) // Alert anything waiting on the connection process to complete
		c.actionCompleted = nil  // Be tidy
		c.Unlock()
	}()

	// Status may have moved to disconnecting in the interim (i.e. at users request)
	if c.status == disconnecting {
		return errAbortConnection
	}
	if success {
		c.status = connected
	} else {
		c.status = disconnected
	}
	return nil
}

// Disconnecting - should be called when beginning the disconnection process (cleanup etc.).
// Can be called from ANY status and the end result will always be a status of disconnected
// Note that if a connection/reconnection attempt is in progress this function will set the status to `disconnecting`
// then block until the connection process completes (or aborts).
// Returns a function that MUST be called when the operation is complete (assumed to always be successful!)
func (c *connectionStatus) Disconnecting() (disconnectCompletedFn, error) {
	c.Lock()
	if c.status == disconnected {
		c.Unlock()
		return nil, errAlreadyDisconnected // May not always be treated as an error
	}
	if c.status == disconnecting { // Need to wait for existing process to complete
		c.willReconnect = false // Ensure that the existing disconnect process will not reconnect
		disConnectDone := c.actionCompleted
		c.Unlock()
		<-disConnectDone                   // Wait for existing operation to complete
		return nil, errAlreadyDisconnected // Well we are now!
	}

	prevStatus := c.status
	c.status = disconnecting

	// We may need to wait for connection/reconnection process to complete (they should regularly check the status)
	if prevStatus == connecting || prevStatus == reconnecting {
		connectDone := c.actionCompleted
		c.Unlock() // Safe because the only way to leave the disconnecting status is via this function
		<-connectDone

		if prevStatus == reconnecting && !c.willReconnect {
			return nil, errAlreadyDisconnected // Following connectionLost process we will be disconnected
		}
		c.Lock()
	}
	c.actionCompleted = make(chan struct{})
	c.Unlock()
	return c.disconnectionCompleted, nil
}

// disconnectionCompleted is an internal function (it is returned by functions that set the status to disconnecting)
func (c *connectionStatus) disconnectionCompleted() {
	c.Lock()
	defer c.Unlock()
	c.status = disconnected
	close(c.actionCompleted) // Alert anything waiting on the connection process to complete
	c.actionCompleted = nil
}

// ConnectionLost - should be called when the connection is lost.
// This really only differs from Disconnecting in that we may transition into a reconnection (but that could be
// cancelled something else calls Disconnecting in the meantime).
// The returned function should be called when cleanup is completed. It will return a function to be called when
// reconnect completes (or nil if no reconnect requested/disconnect called in the interim).
// Note: This function may block if a connection is in progress (the move to connected will be rejected)
func (c *connectionStatus) ConnectionLost(willReconnect bool) (connectionLostHandledFn, error) {
	c.Lock()
	defer c.Unlock()
	if c.status == disconnected {
		return nil, errAlreadyDisconnected
	}
	if c.status == disconnecting { // its expected that connection lost will be called during the disconnection process
		return nil, errDisconnectionInProgress
	}

	c.willReconnect = willReconnect
	prevStatus := c.status
	c.status = disconnecting

	// There is a slight possibility that a connection attempt is in progress (connection up and goroutines started but
	// status not yet changed). By changing the status we ensure that process will exit cleanly
	if prevStatus == connecting || prevStatus == reconnecting {
		connectDone := c.actionCompleted
		c.Unlock() // Safe because the only way to leave the disconnecting status is via this function
		<-connectDone
		c.Lock()
		if !willReconnect {
			// In this case the connection will always be aborted so there is nothing more for us to do
			return nil, errAlreadyDisconnected
		}
	}
	c.actionCompleted = make(chan struct{})

	return c.getConnectionLostHandler(willReconnect), nil
}

// getConnectionLostHandler is an internal function. It returns the function to be returned by ConnectionLost
func (c *connectionStatus) getConnectionLostHandler(reconnectRequested bool) connectionLostHandledFn {
	return func(proceed bool) (connCompletedFn, error) {
		// Note that connCompletedFn will only be provided if both reconnectRequested and proceed are true
		c.Lock()
		defer c.Unlock()

		// `Disconnecting()` may have been called while the disconnection was being processed (this makes it permanent!)
		if !c.willReconnect || !proceed {
			c.status = disconnected
			close(c.actionCompleted) // Alert anything waiting on the connection process to complete
			c.actionCompleted = nil
			if !reconnectRequested || !proceed {
				return nil, nil
			}
			return nil, errDisconnectionRequested
		}

		c.status = reconnecting
		return c.connected, nil // Note that c.actionCompleted is still live and will be closed in connected
	}
}

// forceConnectionStatus - forces the connection status to the specified value.
// This should only be used when there is no alternative (i.e. only in tests and to recover from situations that
// are unexpected)
func (c *connectionStatus) forceConnectionStatus(s status) {
	c.Lock()
	defer c.Unlock()
	c.status = s
}
