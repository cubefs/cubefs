/*
 * Regression Test Case
 * - fix: overlapping extents lost due to concurrent client appending keys
 *
 * Steps:
 * - mount the same volume to <path1> and <path2>
 * - go run main.go <path1>/testfile <path2>/testfile
 *
 * Expected results:
 * - Pass: fsync outPath1 gets IO error due to conflict extents.
 * - Fail: fsync succedded, but reading the file gets IO error due toextents not exist.
 */

package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Invalid num of args, expecting 2 args!\n")
		return
	}

	// Note that outPath1 and outPath2 should point to the same underlying file
	// in ChubaoFS but through different mount point.
	outPath1 := os.Args[1]
	outPath2 := os.Args[2]
	data := "aoifjiwjefojwofoiwenfowepojpjoipgnoirngo\n"

	file1, err := os.OpenFile(outPath1, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file1.Close()
	file2, err := os.OpenFile(outPath2, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file2.Close()

	// Need to write to normal extent, so avoid writing to the beginning 1M
	file1.Seek(1*1024*1024, 0)
	file2.Seek(1*1024*1024, 0)

	_, err = file1.Write([]byte(data))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err = file1.Sync(); err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 2; i++ {
		_, err = file2.Write([]byte(data))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	if err = file2.Sync(); err != nil {
		fmt.Println(err)
		return
	}

	_, err = file1.Write([]byte(data))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err = file1.Sync(); err != nil {
		fmt.Println(err)
		return
	}

	return
}
