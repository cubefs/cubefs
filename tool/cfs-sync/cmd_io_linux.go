package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"
	"time"
)

func runRead(args []string) {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	master := fs.String("master", "", "Comma-separated master addresses")
	vol := fs.String("vol", "", "Volume name (required)")
	filePath := fs.String("path", "", "CubeFS file path to read (required)")
	out := fs.String("out", "-", "Output file path; '-' writes to stdout")
	bs := fs.Int("bs", 4*1024*1024, "Read block size in bytes")
	offset := fs.Int64("offset", 0, "Start offset in bytes")
	size := fs.Int64("size", 0, "Bytes to read; 0 means read to EOF")
	logDir := fs.String("log-dir", "/tmp/cfs-sync-logs", "SDK log directory")
	logLevel := fs.String("log-level", "WARN", "Log level: DEBUG INFO WARN ERROR")
	_ = fs.Parse(args)

	if *vol == "" || *filePath == "" {
		fmt.Fprintln(os.Stderr, "error: --vol and --path are required")
		fs.Usage()
		os.Exit(1)
	}

	cfg, err := loadCLIConfig()
	dieOnErr(err)
	masters, err := resolveMasters(*master, cfg)
	dieOnErr(err)

	c, err := newCFSClient(masters, *vol, *logDir, *logLevel)
	dieOnErr(err)
	defer c.close()

	fileSize, _, serr := c.getAttr(*filePath)
	dieOnErr(serr)

	readSize := *size
	if readSize == 0 || *offset+readSize > int64(fileSize) {
		readSize = int64(fileSize) - *offset
	}
	if readSize <= 0 {
		return
	}

	f, err := c.openFile(*filePath, syscall.O_RDONLY, 0)
	dieOnErr(err)
	defer f.closeFile()

	var w io.Writer
	if *out == "-" {
		w = os.Stdout
	} else {
		wf, ferr := os.Create(*out)
		dieOnErr(ferr)
		defer wf.Close()
		w = wf
	}

	buf := make([]byte, *bs)
	off := *offset
	remaining := readSize
	start := time.Now()

	for remaining > 0 {
		toRead := int64(*bs)
		if toRead > remaining {
			toRead = remaining
		}
		n, rerr := f.readFile(buf[:toRead], off)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				dieOnErr(werr)
			}
		}
		if rerr == io.EOF || n == 0 {
			break
		}
		dieOnErr(rerr)
		off += int64(n)
		remaining -= int64(n)
	}

	elapsed := time.Since(start)
	actual := readSize - remaining
	if *out != "-" {
		printThroughput("read", actual, elapsed)
	}
}

func runWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	master := fs.String("master", "", "Comma-separated master addresses")
	vol := fs.String("vol", "", "Volume name (required)")
	filePath := fs.String("path", "", "CubeFS destination file path (required)")
	in := fs.String("in", "-", "Input file path; '-' reads from stdin")
	bs := fs.Int("bs", 4*1024*1024, "Write block size in bytes")
	overwrite := fs.Bool("overwrite", false, "Overwrite if file already exists")
	logDir := fs.String("log-dir", "/tmp/cfs-sync-logs", "SDK log directory")
	logLevel := fs.String("log-level", "WARN", "Log level: DEBUG INFO WARN ERROR")
	_ = fs.Parse(args)

	if *vol == "" || *filePath == "" {
		fmt.Fprintln(os.Stderr, "error: --vol and --path are required")
		fs.Usage()
		os.Exit(1)
	}

	cfg, err := loadCLIConfig()
	dieOnErr(err)
	masters, err := resolveMasters(*master, cfg)
	dieOnErr(err)

	c, err := newCFSClient(masters, *vol, *logDir, *logLevel)
	dieOnErr(err)
	defer c.close()

	dir := path.Dir(*filePath)
	if dir != "" && dir != "." && dir != "/" {
		if merr := c.mkdirs(dir, 0o755); merr != nil {
			fmt.Fprintf(os.Stderr, "warn: mkdirs %s: %v\n", dir, merr)
		}
	}

	flags := syscall.O_WRONLY | syscall.O_CREAT
	if *overwrite {
		flags |= syscall.O_TRUNC
	} else {
		flags |= syscall.O_EXCL
	}
	f, err := c.openFile(*filePath, flags, 0o644)
	dieOnErr(err)
	defer func() {
		_ = f.flush()
		_ = f.closeFile()
	}()

	var r io.Reader
	if *in == "-" {
		r = os.Stdin
	} else {
		rf, rerr := os.Open(*in)
		dieOnErr(rerr)
		defer rf.Close()
		r = rf
	}

	buf := make([]byte, *bs)
	off := int64(0)
	total := int64(0)
	start := time.Now()

	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			wn, werr := f.writeFile(buf[:n], off)
			dieOnErr(werr)
			off += int64(wn)
			total += int64(wn)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		dieOnErr(rerr)
	}

	elapsed := time.Since(start)
	printThroughput("write", total, elapsed)
}
