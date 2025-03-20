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

package common

type (
	Bool   struct{ V bool }
	Int    struct{ V int64 }
	Uint   struct{ V uint64 }
	Float  struct{ V float64 }
	String struct{ V string }
)

func (b *Bool) Key(key string) *Argument { return NewArgument(key, &b.V) }
func (b *Bool) Enable() *Argument        { return b.Key("enable") }
func (b *Bool) Status() *Argument        { return b.Key("status") }
func (b *Bool) All() *Argument           { return b.Key("all") }

func (i *Int) Key(key string) *Argument { return NewArgument(key, &i.V) }
func (i *Int) ID() *Argument            { return i.Key("id") }
func (i *Int) ExtentID() *Argument      { return i.Key("extentID") }
func (i *Int) Count() *Argument         { return i.Key("count") }
func (i *Int) Flow() *Argument          { return i.Key("flow") }
func (i *Int) Iocc() *Argument          { return i.Key("iocc") }
func (i *Int) Factor() *Argument        { return i.Key("factor") }

func (u *Uint) Key(key string) *Argument { return NewArgument(key, &u.V) }
func (u *Uint) ID() *Argument            { return u.Key("id") }
func (u *Uint) PID() *Argument           { return u.Key("pid") }
func (u *Uint) PartitionID() *Argument   { return u.Key("partitionID") }
func (u *Uint) Ino() *Argument           { return u.Key("ino") }
func (u *Uint) ParentIno() *Argument     { return u.Key("parentIno") }

func (f *Float) Key(key string) *Argument { return NewArgument(key, &f.V) }

func (s *String) Key(key string) *Argument { return NewArgument(key, &s.V) }
func (s *String) Disk() *Argument          { return s.Key("disk") }
func (s *String) DiskPath() *Argument      { return s.Key("diskPath") }
func (s *String) Addr() *Argument          { return s.Key("addr") }
func (s *String) ZoneName() *Argument      { return s.Key("zoneName") }
