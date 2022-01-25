package errors

// code for access
const (
	CodeAccessReadRequestBody  = 466 // read request body error
	CodeAccessReadConflictBody = 499 // read conflict body error
	CodeAccessUnexpect         = 550 // unexpect
	CodeAccessServiceDiscovery = 551 // service discovery for access api client
	CodeAccessLimited          = 552 // read write limited for access api client
	CodeAccessExceedSize       = 553 // exceed max size
)

// errro of access
var (
	ErrAccessReadRequestBody  = Error(CodeAccessReadRequestBody)
	ErrAccessReadConflictBody = Error(CodeAccessReadConflictBody)
	ErrAccessUnexpect         = Error(CodeAccessUnexpect)
	ErrAccessServiceDiscovery = Error(CodeAccessServiceDiscovery)
	ErrAccessLimited          = Error(CodeAccessLimited)
	ErrAccessExceedSize       = Error(CodeAccessExceedSize)
)
