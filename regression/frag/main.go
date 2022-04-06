/*
 * Fragmentation Test Case
 *
 * This test case will create tons of individual extents within just a few files. And deleting these few files will generate overwhelming batch delete extents requests which may lead to datenode panic because of exceeding thread limit.
 *
 * This is because large number of goroutines are waiting for IO, so the golang runtime will have to create new threads to run goroutines until the number of threads reach a limit and panic.
 *
 */

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	numFiles := 1000
	fileSize := 1024 * 1024 * 128

	if len(os.Args) != 2 {
		fmt.Println("Invalid num of args, expecting 1")
		os.Exit(1)
	}

	testpath, err := filepath.Abs(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	data := []byte("sssssssssssssssssssssssssssssssss")

	for i := 0; i < numFiles; i++ {
		filepath := filepath.Join(testpath, fmt.Sprintf("%05d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()

			f, e := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
			if e != nil {
				fmt.Printf("[%v]: failed to open file, err[%v]\n", filepath, e)
				return
			}
			defer f.Close()

			var offset int64
			for {
				if _, e = f.WriteAt(data, offset); e != nil {
					fmt.Printf("[%v]: failed to write to file, offset[%v] err[%v]\n", filepath, offset, e)
					return
				}

				if offset%int64(4*1024*1024) == 0 {
					if e = f.Sync(); e != nil {
						fmt.Printf("[%v]: failed to sync file, err[%v]\n", filepath, e)
						return
					}
				}

				offset += 4096
				if int(offset) >= fileSize {
					fmt.Printf("[%v]: finished!\n", filepath)
					break
				}
			}
		}()
	}

	wg.Wait()
	return
}
