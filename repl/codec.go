package repl

import (
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/util/blackmagic"
)

var ReplArgSplit = "/"

// Encode follower hosts and quorum value into a bytes.
// Format: {HOST1}/{HOST2}/{HOST3}/{QUORUM}
func EncodeReplPacketArg(followers []string, quorum int) []byte {
	return blackmagic.StringToBytes(strings.Join(followers, ReplArgSplit) + ReplArgSplit + strconv.Itoa(quorum))
}

// Decode follower hosts and quorum from a bytes.
func DecodeReplPacketArg(raw []byte) (followers []string, quorum int) {
	parts := strings.SplitN(blackmagic.BytesToString(raw), ReplArgSplit, -1)
	followers = parts[:len(parts)-1]
	if len(parts) >= 2 {
		quorum, _ = strconv.Atoi(parts[len(parts)-1])
	}
	if quorum < 0 || quorum > len(followers)+1 {
		quorum = 0
	}
	return
}
