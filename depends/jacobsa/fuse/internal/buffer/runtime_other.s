// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build amd64 arm64 ppc64 ppc64le
// +build !go1.8

// Assembly code isn't subject to visibility restrictions, so we can jump
// directly into package runtime.
//
// Technique copied from here:
//     https://github.com/golang/go/blob/d8c6dac/src/os/signal/sig.s

#include "textflag.h"

#ifdef GOARCH_ppc64
#define JMP BR
#endif
#ifdef GOARCH_ppc64le
#define JMP BR
#endif

TEXT ·memclr(SB),NOSPLIT,$0-16
	JMP runtime·memclr(SB)

TEXT ·memmove(SB),NOSPLIT,$0-24
	JMP runtime·memmove(SB)
