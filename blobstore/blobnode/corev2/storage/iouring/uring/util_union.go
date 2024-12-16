// Copyright 2024 The CubeFS Authors.
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

package uring

import "unsafe"

type union64 [8]byte

func (u *union64) PutUnsafe(v unsafe.Pointer) { putUnsafe(unsafe.Pointer(u), v) }
func (u *union64) PutUintptr(v uintptr)       { putUintptr(unsafe.Pointer(u), v) }
func (u *union64) PutUint64(v uint64)         { putUint64(unsafe.Pointer(u), v) }
func (u *union64) PutUint32(v uint32)         { putUint32(unsafe.Pointer(u), v) }
func (u *union64) PutUint16(v uint16)         { putUint16(unsafe.Pointer(u), v) }
func (u *union64) PutUint8(v uint8)           { putUint8(unsafe.Pointer(u), v) }

func (u *union64) PutInt32(v int32) { putInt32(unsafe.Pointer(u), v) }

func (u *union64) Unsafe() unsafe.Pointer { return unsafe.Pointer(u) }
func (u *union64) Uint64() uint64         { return *(*uint64)(unsafe.Pointer(u)) }
func (u *union64) Uint32() uint32         { return *(*uint32)(unsafe.Pointer(u)) }
func (u *union64) Uint16() uint16         { return *(*uint16)(unsafe.Pointer(u)) }
func (u *union64) Uint8() uint8           { return *(*uint8)(unsafe.Pointer(u)) }

type union32 [4]byte

func (u *union32) PutUnsafe(v unsafe.Pointer) { putUnsafe(unsafe.Pointer(u), v) }
func (u *union32) PutUintptr(v uintptr)       { putUintptr(unsafe.Pointer(u), uintptr(uint32(v))) }
func (u *union32) PutUint64(v uint64)         { putUint32(unsafe.Pointer(u), uint32(v)) }
func (u *union32) PutUint32(v uint32)         { putUint32(unsafe.Pointer(u), v) }
func (u *union32) PutUint16(v uint16)         { putUint16(unsafe.Pointer(u), v) }
func (u *union32) PutUint8(v uint8)           { putUint8(unsafe.Pointer(u), v) }

func (u *union32) PutInt32(v int32) { putInt32(unsafe.Pointer(u), v) }

func (u *union32) Unsafe() unsafe.Pointer { return unsafe.Pointer(u) }
func (u *union32) Uint64() uint64         { return *(*uint64)(unsafe.Pointer(u)) }
func (u *union32) Uint32() uint32         { return *(*uint32)(unsafe.Pointer(u)) }
func (u *union32) Uint16() uint16         { return *(*uint16)(unsafe.Pointer(u)) }
func (u *union32) Uint8() uint8           { return *(*uint8)(unsafe.Pointer(u)) }

type union16 [2]byte

func (u *union16) PutUnsafe(v unsafe.Pointer) { putUnsafe(unsafe.Pointer(u), v) }
func (u *union16) PutUintptr(v uintptr)       { putUintptr(unsafe.Pointer(u), uintptr(uint16(v))) }
func (u *union16) PutUint64(v uint64)         { putUint16(unsafe.Pointer(u), uint16(v)) }
func (u *union16) PutUint32(v uint32)         { putUint16(unsafe.Pointer(u), uint16(v)) }
func (u *union16) PutUint16(v uint16)         { putUint16(unsafe.Pointer(u), v) }
func (u *union16) PutUint8(v uint8)           { putUint8(unsafe.Pointer(u), v) }

func (u *union16) Unsafe() unsafe.Pointer { return unsafe.Pointer(u) }
func (u *union16) Uint64() uint64         { return *(*uint64)(unsafe.Pointer(u)) }
func (u *union16) Uint32() uint32         { return *(*uint32)(unsafe.Pointer(u)) }
func (u *union16) Uint16() uint16         { return *(*uint16)(unsafe.Pointer(u)) }
func (u *union16) Uint8() uint8           { return *(*uint8)(unsafe.Pointer(u)) }
