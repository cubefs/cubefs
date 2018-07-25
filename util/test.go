package util

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {

	f, err := os.Open("2.log")
	if err != nil {
		return
	}
	reader := bufio.NewReader(f)
	allKey := make([]string, 0)
	allKeymap := make(map[string]int)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		arr := strings.Split(line, " ")
		inode := arr[1]
		partiid := arr[3]
		extentid := arr[4]
		size := arr[5]
		fmt.Println(size)
		sizestr := strings.Split(strings.Split(size, "Size(")[1], ")")[0]
		sizeint, _ := strconv.Atoi(sizestr)
		key := fmt.Sprintf("%v_%v_%v", inode, partiid, extentid)
		_, ok := allKeymap[key]
		if !ok {
			allKeymap[key] = sizeint
			allKey = append(allKey, key)
			continue
		} else {
			allKeymap[key] = sizeint
		}
	}
	sumsize := 0
	for _, key := range allKey {
		sumsize += allKeymap[key]
		fmt.Println(fmt.Sprintf("%v:%v", key, allKeymap[key]))
	}
	fmt.Println(sumsize)
}
