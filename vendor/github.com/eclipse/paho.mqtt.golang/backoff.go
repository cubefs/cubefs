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
 *    Matt Brittan
 *    Daichi Tomaru
 */

package mqtt

import (
	"sync"
	"time"
)

// Controller for sleep with backoff when the client attempts reconnection
// It has statuses for each situations cause reconnection.
type backoffController struct {
	sync.RWMutex
	statusMap map[string]*backoffStatus
}

type backoffStatus struct {
	lastSleepPeriod time.Duration
	lastErrorTime   time.Time
}

func newBackoffController() *backoffController {
	return &backoffController{
		statusMap: map[string]*backoffStatus{},
	}
}

// Calculate next sleep period from the specified parameters.
// Returned values are next sleep period and whether the error situation is continual.
// If connection errors continuouslly occurs, its sleep period is exponentially increased.
// Also if there is a lot of time between last and this error, sleep period is initialized.
func (b *backoffController) getBackoffSleepTime(
	situation string, initSleepPeriod time.Duration, maxSleepPeriod time.Duration, processTime time.Duration, skipFirst bool,
) (time.Duration, bool) {
	// Decide first sleep time if the situation is not continual. 
	var firstProcess = func(status *backoffStatus, init time.Duration, skip bool) (time.Duration, bool) {
		if skip {
			status.lastSleepPeriod = 0
			return 0, false
		}
		status.lastSleepPeriod = init
		return init, false
	}

	// Prioritize maxSleep.
	if initSleepPeriod > maxSleepPeriod {
		initSleepPeriod = maxSleepPeriod
	}
	b.Lock()
	defer b.Unlock()

	status, exist := b.statusMap[situation]
	if !exist {
		b.statusMap[situation] = &backoffStatus{initSleepPeriod, time.Now()}
		return firstProcess(b.statusMap[situation], initSleepPeriod, skipFirst)
	}

	oldTime := status.lastErrorTime
	status.lastErrorTime = time.Now()

	// When there is a lot of time between last and this error, sleep period is initialized.
	if status.lastErrorTime.Sub(oldTime) > (processTime * 2 + status.lastSleepPeriod) {
		return firstProcess(status, initSleepPeriod, skipFirst)
	}

	if status.lastSleepPeriod == 0 {
		status.lastSleepPeriod = initSleepPeriod
		return initSleepPeriod, true
	}

	if nextSleepPeriod := status.lastSleepPeriod * 2; nextSleepPeriod <= maxSleepPeriod {
		status.lastSleepPeriod = nextSleepPeriod
	} else {
		status.lastSleepPeriod = maxSleepPeriod
	}

	return status.lastSleepPeriod, true
}

// Execute sleep the time returned from getBackoffSleepTime.
func (b *backoffController) sleepWithBackoff(
	situation string, initSleepPeriod time.Duration, maxSleepPeriod time.Duration, processTime time.Duration, skipFirst bool,
) (time.Duration, bool) {
	sleep, isFirst := b.getBackoffSleepTime(situation, initSleepPeriod, maxSleepPeriod, processTime, skipFirst)
	if sleep != 0 {
		time.Sleep(sleep)
	}
	return sleep, isFirst
}
