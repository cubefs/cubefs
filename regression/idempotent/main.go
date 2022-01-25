/*
 * Regression Test Case
 * - fix: makes AppendExtentKeyWithCheck request idempotent
 *
 * Steps:
 * - modify metanode source code to let the second AppendExtentKeyWithCheck request succeeds,
 *   but responds failed status to the client. This simulate the scenarios of network failure
 *   and let the client retries. How to modify can be seen as follows:
 *
 * diff --cc metanode/partition_op_extent.go
 * index 2f29cf1d,2f29cf1d..69f7947d
 * --- a/metanode/partition_op_extent.go
 * +++ b/metanode/partition_op_extent.go
 * @@@ -21,6 -21,6 +21,8 @@@ import
 *         "github.com/cubefs/cubefs/proto"
 *   )
 *
 * ++var faultInjection int32
 * ++
 *   // ExtentAppend appends an extent.
 *   func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
 *         ino := NewInode(req.Inode, 0)
 * @@@ -61,6 -61,6 +63,10 @@@ func (mp *metaPartition) ExtentAppendWi
 *                 p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
 *                 return
 *         }
 * ++      if err == nil && faultInjection == 1 {
 * ++              resp = proto.OpAgain
 * ++      }
 * ++      faultInjection++
 *         p.PacketErrorWithBody(resp.(uint8), nil)
 *         return
 *   }
 *
 * - deploy cluster using "run_docker.sh -r"
 * - mount volume in <path1>
 * - go run main.go <path1>/testfile
 *
 * Expected results:
 * - Pass: There is no error.
 * - Fail: The second fsync gets an IO error due to conflict extents.
 */

package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Invalid num of args, expecting 1")
		return
	}

	outPath1 := os.Args[1]
	data1 := "1111\n"
	data2 := "2222\n"

	file1, err := os.OpenFile(outPath1, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file1.Close()

	// round 1
	_, err = file1.WriteAt([]byte(data1), 0)
	if err != nil {
		fmt.Println("1: ", err)
		return
	}
	if err = file1.Sync(); err != nil {
		fmt.Println("1: ", err)
		return
	}

	// round 2
	_, err = file1.WriteAt([]byte(data2), 0)
	if err != nil {
		fmt.Println("2: ", err)
		return
	}
	if err = file1.Sync(); err != nil {
		fmt.Println("2: ", err)
		return
	}

	return
}
