package metanode

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type DelExtFile []os.FileInfo

func (del DelExtFile) Len() int {
	return len(del)
}

func (del DelExtFile) Swap(i, j int) {
	del[i], del[j] = del[j], del[i]
}

func (del DelExtFile) Less(i, j int) bool {

	idx1 := getDelExtFileIdx(del[i].Name())
	idx2 := getDelExtFileIdx(del[j].Name())

	return idx1 < idx2
}

func getDelExtFileIdx(name string) int64 {
	arr := strings.Split(name, "_")
	size := len(arr)
	if size < 2 {
		panic(fmt.Errorf("file name is not legal, %s", name))
	}

	idx, err := strconv.ParseInt(arr[size-1], 10, 64)
	if err != nil {
		panic(fmt.Errorf("file name is not legal, %s", name))
	}

	return idx
}

func sortDelExtFileInfo(files []os.FileInfo) []os.FileInfo {
	newFiles := make([]os.FileInfo, 0)

	for _, info := range files {
		if info.IsDir() {
			continue
		}

		if strings.HasPrefix(info.Name(), prefixDelExtent) {
			newFiles = append(newFiles, info)
		}
	}

	if len(newFiles) <= 1 {
		return newFiles
	}

	sort.Sort(DelExtFile(newFiles))

	return newFiles
}
