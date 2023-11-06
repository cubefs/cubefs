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
	"sort"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

// OrderedMemoryStore uses a map internally so the order in which All() returns packets is
// undefined. OrderedMemoryStore resolves this by storing the time the message is added
// and sorting based upon this.

// storedMessage encapsulates a message and the time it was initially stored
type storedMessage struct {
	ts  time.Time
	msg packets.ControlPacket
}

// OrderedMemoryStore implements the store interface to provide a "persistence"
// mechanism wholly stored in memory. This is only useful for
// as long as the client instance exists.
type OrderedMemoryStore struct {
	sync.RWMutex
	messages map[string]storedMessage
	opened   bool
}

// NewOrderedMemoryStore returns a pointer to a new instance of
// OrderedMemoryStore, the instance is not initialized and ready to
// use until Open() has been called on it.
func NewOrderedMemoryStore() *OrderedMemoryStore {
	store := &OrderedMemoryStore{
		messages: make(map[string]storedMessage),
		opened:   false,
	}
	return store
}

// Open initializes a OrderedMemoryStore instance.
func (store *OrderedMemoryStore) Open() {
	store.Lock()
	defer store.Unlock()
	store.opened = true
	DEBUG.Println(STR, "OrderedMemoryStore initialized")
}

// Put takes a key and a pointer to a Message and stores the
// message.
func (store *OrderedMemoryStore) Put(key string, message packets.ControlPacket) {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use memory store, but not open")
		return
	}
	store.messages[key] = storedMessage{ts: time.Now(), msg: message}
}

// Get takes a key and looks in the store for a matching Message
// returning either the Message pointer or nil.
func (store *OrderedMemoryStore) Get(key string) packets.ControlPacket {
	store.RLock()
	defer store.RUnlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use memory store, but not open")
		return nil
	}
	mid := mIDFromKey(key)
	m, ok := store.messages[key]
	if !ok || m.msg == nil {
		CRITICAL.Println(STR, "OrderedMemoryStore get: message", mid, "not found")
	} else {
		DEBUG.Println(STR, "OrderedMemoryStore get: message", mid, "found")
	}
	return m.msg
}

// All returns a slice of strings containing all the keys currently
// in the OrderedMemoryStore.
func (store *OrderedMemoryStore) All() []string {
	store.RLock()
	defer store.RUnlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use memory store, but not open")
		return nil
	}
	type tsAndKey struct {
		ts  time.Time
		key string
	}

	tsKeys := make([]tsAndKey, 0, len(store.messages))
	for k, v := range store.messages {
		tsKeys = append(tsKeys, tsAndKey{ts: v.ts, key: k})
	}
	sort.Slice(tsKeys, func(a int, b int) bool { return tsKeys[a].ts.Before(tsKeys[b].ts) })

	keys := make([]string, len(tsKeys))
	for i := range tsKeys {
		keys[i] = tsKeys[i].key
	}
	return keys
}

// Del takes a key, searches the OrderedMemoryStore and if the key is found
// deletes the Message pointer associated with it.
func (store *OrderedMemoryStore) Del(key string) {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use memory store, but not open")
		return
	}
	mid := mIDFromKey(key)
	_, ok := store.messages[key]
	if !ok {
		WARN.Println(STR, "OrderedMemoryStore del: message", mid, "not found")
	} else {
		delete(store.messages, key)
		DEBUG.Println(STR, "OrderedMemoryStore del: message", mid, "was deleted")
	}
}

// Close will disallow modifications to the state of the store.
func (store *OrderedMemoryStore) Close() {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to close memory store, but not open")
		return
	}
	store.opened = false
	DEBUG.Println(STR, "OrderedMemoryStore closed")
}

// Reset eliminates all persisted message data in the store.
func (store *OrderedMemoryStore) Reset() {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to reset memory store, but not open")
	}
	store.messages = make(map[string]storedMessage)
	WARN.Println(STR, "OrderedMemoryStore wiped")
}
