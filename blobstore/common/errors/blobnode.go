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

package errors

const (
	CodeInvalidParam   = 600
	CodeAlreadyExist   = 601
	CodeOutOfLimit     = 602
	CodeInternal       = 603
	CodeOverload       = 604
	CodePathNotExist   = 605
	CodePathNotEmpty   = 606
	CodePathFindOnline = 607

	CodeDiskNotFound  = 611
	CodeDiskBroken    = 613
	CodeInvalidDiskId = 614
	CodeDiskNoSpace   = 615

	CodeVuidNotFound     = 621
	CodeVUIDReadonly     = 622
	CodeVUIDRelease      = 623
	CodeVuidNotMatch     = 624
	CodeChunkNotReadonly = 625
	CodeChunkNotNormal   = 626
	CodeChunkNoSpace     = 627
	CodeChunkCompacting  = 628
	CodeInvalidChunkId   = 630
	CodeTooManyChunks    = 632
	CodeChunkInuse       = 633
	CodeSizeOverBurst    = 634

	CodeBidNotFound          = 651
	CodeShardSizeTooLarge    = 652
	CodeShardNotMarkDelete   = 653
	CodeShardMarkDeleted     = 654
	CodeShardInvalidOffset   = 655
	CodeShardListExceedLimit = 656
	CodeShardInvalidBid      = 657
)

var (
	ErrInvalidParam   = Error(CodeInvalidParam)
	ErrAlreadyExist   = Error(CodeAlreadyExist)
	ErrOutOfLimit     = Error(CodeOutOfLimit)
	ErrOverload       = Error(CodeOverload)
	ErrPathNotExist   = Error(CodePathNotExist)
	ErrPathNotEmpty   = Error(CodePathNotEmpty)
	ErrPathFindOnline = Error(CodePathFindOnline)

	ErrNoSuchDisk    = Error(CodeDiskNotFound)
	ErrDiskBroken    = Error(CodeDiskBroken)
	ErrDiskNoSpace   = Error(CodeDiskNoSpace)
	ErrInvalidDiskId = Error(CodeInvalidDiskId)

	ErrNoSuchVuid       = Error(CodeVuidNotFound)
	ErrReadonlyVUID     = Error(CodeVUIDReadonly)
	ErrReleaseVUID      = Error(CodeVUIDRelease)
	ErrVuidNotMatch     = Error(CodeVuidNotMatch)
	ErrChunkNotReadonly = Error(CodeChunkNotReadonly)
	ErrChunkNotNormal   = Error(CodeChunkNotNormal)
	ErrChunkNoSpace     = Error(CodeChunkNoSpace)
	ErrChunkInCompact   = Error(CodeChunkCompacting)
	ErrInvalidChunkId   = Error(CodeInvalidChunkId)
	ErrTooManyChunks    = Error(CodeTooManyChunks)
	ErrChunkInuse       = Error(CodeChunkInuse)
	ErrSizeOverBurst    = Error(CodeSizeOverBurst)

	ErrNoSuchBid            = Error(CodeBidNotFound)
	ErrShardSizeTooLarge    = Error(CodeShardSizeTooLarge)
	ErrShardNotMarkDelete   = Error(CodeShardNotMarkDelete)
	ErrShardMarkDeleted     = Error(CodeShardMarkDeleted)
	ErrShardInvalidOffset   = Error(CodeShardInvalidOffset)
	ErrShardListExceedLimit = Error(CodeShardListExceedLimit)
	ErrShardInvalidBid      = Error(CodeShardInvalidBid)
)
