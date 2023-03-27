package master

import "strings"

var mixedMetaNodeMap = map[string]bool{
}

func IsMixedMetaNode(addr string) bool {
	if len(mixedMetaNodeMap) == 0 {
		return false
	}
	arr := strings.Split(addr, ":")
	if len(arr) == 0 {
		return false
	}
	return mixedMetaNodeMap[arr[0]]
}
