package errors

const (
	CodeInvalidParam = 600
	CodeAlreadyExist = 601
	CodeOutOfLimit   = 602
	CodeInternal     = 603
	CodeOverload     = 604

	CodeDiskNotFound  = 611
	CodeDiskReadOnly  = 612
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

	CodeBidNotFound          = 651
	CodeShardSizeTooLarge    = 652
	CodeShardNotMarkDelete   = 653
	CodeShardMarkDeleted     = 654
	CodeShardInvalidOffset   = 655
	CodeShardListExceedLimit = 656
	CodeShardInvalidBid      = 657
)

var (
	ErrInvalidParam = Error(CodeInvalidParam)
	ErrAlreadyExist = Error(CodeAlreadyExist)
	ErrOutOfLimit   = Error(CodeOutOfLimit)
	ErrOverload     = Error(CodeOverload)

	ErrNoSuchDisk    = Error(CodeDiskNotFound)
	ErrReadOnlyDisk  = Error(CodeDiskReadOnly)
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

	ErrNoSuchBid            = Error(CodeBidNotFound)
	ErrShardSizeTooLarge    = Error(CodeShardSizeTooLarge)
	ErrShardNotMarkDelete   = Error(CodeShardNotMarkDelete)
	ErrShardMarkDeleted     = Error(CodeShardMarkDeleted)
	ErrShardInvalidOffset   = Error(CodeShardInvalidOffset)
	ErrShardListExceedLimit = Error(CodeShardListExceedLimit)
	ErrShardInvalidBid      = Error(CodeShardInvalidBid)
)
