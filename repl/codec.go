package repl

import (
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util/blackmagic"
)

var ReplArgSplit = "/"

// Encode follower hosts and quorum value into a bytes.
// Format: {HOST1}/{HOST2}/{HOST3}/{QUORUM}
func EncodeReplPacketArg(followers []string, quorum int) []byte {
	return []byte(strings.Join(followers, ReplArgSplit) + ReplArgSplit + strconv.Itoa(quorum))
}

// Decode follower hosts and quorum from a bytes.
func DecodeReplPacketArg(raw []byte) (followers []string, quorum int) {
	parts := strings.SplitN(blackmagic.BytesToString(raw), ReplArgSplit, -1)
	followers = parts[:len(parts)-1]
	if len(parts) >= 2 {
		var convErr error
		// 兼容老版本，老版本以"/"结尾不携带quorum信息，这种情况下不启用quorum.
		if quorum, convErr = strconv.Atoi(parts[len(parts)-1]); convErr != nil {
			quorum = 0
			return
		}
		// 负值quorum为无效值，这种情况下置为0，表示不启用quorum.
	}
	// 进行quorum值合法性校验，以下三种情况quorum值为无效值
	// 1. 负值: quorum < 0
	// 2. 不满足超过复制组成员半数: 0 < quorum <= (len(followers)+1)/2
	// 3. 超过复制组成员总数: quorum > len(followers)+1
	if quorum < 0 || (0 < quorum && (quorum <= (len(followers)+1)/2) || quorum > len(followers)+1) {
		// 不启用quorum
		quorum = 0
	}
	return
}
