package tools

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
)

var (
	filename = flag.String("name", "", "file name")
)

func main() {
	flag.Parse()
	if *filename == "" {
		fmt.Printf("Error: Filename not specified!\n")
		return
	}
	fp, err := os.Open(*filename)
	if err != nil {
		fmt.Printf("Error: open file(%v) err(%v)\n", *filename, err)
		return
	}
	defer fp.Close()

	stat, err := fp.Stat()
	if err != nil {
		fmt.Printf("Error: stat file(%v) err(%v)\n", *filename, err)
		return
	}

	size := stat.Size()
	data := make([]byte, size)
	var n int
	if n, err = fp.Read(data); err != nil {
		fmt.Printf("Error: read file(%v) n(%v) err(%v)\n", *filename, n, err)
		return
	}

	writeCrc := binary.BigEndian.Uint32(data[size-4:])
	actualCrc := crc32.ChecksumIEEE(data[:size-4])
	if writeCrc != actualCrc {
		fmt.Printf("name(%v) crc not match actualCrc(%v) writeCrc(%v) size(%v) readn(%v)\n", *filename, actualCrc, writeCrc, size, n)
		return
	}

	fmt.Println("Done\n")
	return
}
