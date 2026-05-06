package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	master := fs.String("master", "", "Comma-separated master addresses")
	vol := fs.String("vol", "", "Volume name (required unless --flash)")
	benchPath := fs.String("path", "/cfs-sync-bench", "CubeFS directory for bench files")
	mode := fs.String("mode", "read", "Benchmark mode: read, write, rw")
	bs := fs.Int("bs", 4*1024*1024, "Block size in bytes")
	numjobs := fs.Int("numjobs", 1, "Number of parallel workers")
	runtime := fs.Int("runtime", 60, "Duration in seconds")
	size := fs.Int64("size", 1024*1024*1024, "Total data size per job in bytes")
	flash := fs.Bool("flash", false, "Benchmark FlashNode via remotecache (no gosdk)")
	output := fs.String("output", "text", "Output format: text or json")
	logDir := fs.String("log-dir", "/tmp/cfs-sync-logs", "SDK log directory")
	logLevel := fs.String("log-level", "WARN", "Log level: DEBUG INFO WARN ERROR")
	_ = fs.Parse(args)

	cfg, err := loadCLIConfig()
	dieOnErr(err)
	masters, err := resolveMasters(*master, cfg)
	dieOnErr(err)

	if *flash {
		runFlashBench(masters, *mode, *bs, *numjobs, *runtime, *size, *output, *logDir, *logLevel)
		return
	}

	if *vol == "" {
		fmt.Fprintln(os.Stderr, "error: --vol is required (or use --flash for FlashNode bench)")
		fs.Usage()
		os.Exit(1)
	}

	runGOSDKBench(masters, *vol, *benchPath, *mode, *bs, *numjobs, *runtime, *size, *output, *logDir, *logLevel)
}

func runGOSDKBench(masters []string, vol, benchPath, mode string, bs, numjobs, runtime int, size int64, output, logDir, logLevel string) {
	c, err := newCFSClient(masters, vol, logDir, logLevel)
	dieOnErr(err)
	defer c.close()

	if err = c.mkdirs(benchPath, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "warn: mkdirs %s: %v\n", benchPath, err)
	}

	blocksPerJob := int(size) / bs
	if blocksPerJob == 0 {
		blocksPerJob = 1
	}

	var totalBytes int64
	var errCount int64
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(time.Duration(runtime) * time.Second)

	for j := 0; j < numjobs; j++ {
		wg.Add(1)
		go func(jobID int) {
			defer wg.Done()
			fpath := path.Join(benchPath, fmt.Sprintf("job-%d.dat", jobID))
			buf := make([]byte, bs)
			_, _ = cryptorand.Read(buf)

			if mode == "write" || mode == "rw" {
				benchWriteJob(c, fpath, buf, blocksPerJob, deadline, &totalBytes, &errCount)
			}
			if mode == "read" || mode == "rw" {
				benchReadJob(c, fpath, buf, blocksPerJob, deadline, &totalBytes, &errCount)
			}
		}(j)
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := buildResult(mode, bs, numjobs, runtime, totalBytes, elapsed, errCount)
	printResult(res, output)
}

func benchWriteJob(c *cfsClient, fpath string, buf []byte, blocks int, deadline time.Time, total, errCount *int64) {
	flags := syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC
	f, err := c.openFile(fpath, flags, 0o644)
	if err != nil {
		atomic.AddInt64(errCount, 1)
		return
	}
	defer func() {
		_ = f.flush()
		_ = f.closeFile()
	}()

	for i := 0; i < blocks && time.Now().Before(deadline); i++ {
		n, werr := f.writeFile(buf, int64(i)*int64(len(buf)))
		if werr != nil {
			atomic.AddInt64(errCount, 1)
			continue
		}
		atomic.AddInt64(total, int64(n))
	}
}

func benchReadJob(c *cfsClient, fpath string, buf []byte, blocks int, deadline time.Time, total, errCount *int64) {
	f, err := c.openFile(fpath, syscall.O_RDONLY, 0)
	if err != nil {
		atomic.AddInt64(errCount, 1)
		return
	}
	defer f.closeFile()

	for i := 0; i < blocks && time.Now().Before(deadline); i++ {
		n, rerr := f.readFile(buf, int64(i)*int64(len(buf)))
		if rerr != nil && rerr != io.EOF {
			atomic.AddInt64(errCount, 1)
			continue
		}
		atomic.AddInt64(total, int64(n))
	}
}

func runFlashBench(masters []string, mode string, bs, numjobs, runtime int, size int64, output, logDir, logLevel string) {
	c, err := newFlashClient(masters, logDir, logLevel)
	dieOnErr(err)
	defer c.close()

	blocksPerJob := int(size) / bs
	if blocksPerJob == 0 {
		blocksPerJob = 1
	}

	var writeBytes int64
	var readBytes int64
	var writeErrCount int64
	var readErrCount int64
	var firstWriteErr atomic.Value
	var firstReadErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(time.Duration(runtime) * time.Second)
	ctx := context.Background()

	keys := make([][]string, numjobs)
	for j := 0; j < numjobs; j++ {
		keys[j] = make([]string, blocksPerJob)
		for i := range keys[j] {
			keys[j][i] = uuid.New().String()
		}
	}

	var writeElapsed, readElapsed time.Duration

	if mode == "write" || mode == "rw" {
		ws := time.Now()
		for j := 0; j < numjobs; j++ {
			wg.Add(1)
			go func(jobID int) {
				defer wg.Done()
				buf := make([]byte, bs)
				_, _ = cryptorand.Read(buf)
				for _, key := range keys[jobID] {
					if !time.Now().Before(deadline) {
						break
					}
					werr := c.rc.Put(ctx, uuid.New().String(), key, bytes.NewReader(buf), int64(len(buf)))
					if werr != nil {
						firstWriteErr.CompareAndSwap(nil, werr)
						atomic.AddInt64(&writeErrCount, 1)
						continue
					}
					atomic.AddInt64(&writeBytes, int64(bs))
				}
			}(j)
		}
		wg.Wait()
		writeElapsed = time.Since(ws)
		if v := firstWriteErr.Load(); v != nil {
			if atomic.LoadInt64(&writeBytes) == 0 {
				fmt.Fprintf(os.Stderr, "error: flash PUT failed: %v\n", v)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "warn: write errors=%d first=%v\n", atomic.LoadInt64(&writeErrCount), v)
		}
	}

	if mode == "read" || mode == "rw" {
		rs := time.Now()
		for j := 0; j < numjobs; j++ {
			wg.Add(1)
			go func(jobID int) {
				defer wg.Done()
				shuffled := make([]string, len(keys[jobID]))
				copy(shuffled, keys[jobID])
				rand.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
				for _, key := range shuffled {
					if !time.Now().Before(deadline) {
						break
					}
					r, length, _, rerr := c.rc.Get(ctx, uuid.New().String(), key, 0, int64(bs))
					if rerr != nil {
						firstReadErr.CompareAndSwap(nil, rerr)
						atomic.AddInt64(&readErrCount, 1)
						continue
					}
					_, _ = io.Copy(io.Discard, r)
					_ = r.Close()
					atomic.AddInt64(&readBytes, length)
				}
			}(j)
		}
		wg.Wait()
		readElapsed = time.Since(rs)
		if v := firstReadErr.Load(); v != nil {
			fmt.Fprintf(os.Stderr, "warn: read errors=%d first=%v\n", atomic.LoadInt64(&readErrCount), v)
		}
	}

	elapsed := time.Since(start)
	totalBytes := atomic.LoadInt64(&writeBytes) + atomic.LoadInt64(&readBytes)
	errCount := atomic.LoadInt64(&writeErrCount) + atomic.LoadInt64(&readErrCount)

	res := buildResult(mode, bs, numjobs, runtime, totalBytes, elapsed, errCount)
	printFlashResult(res, output, writeBytes, readBytes, writeElapsed, readElapsed, writeErrCount, readErrCount)
	if errCount > 0 {
		os.Exit(1)
	}
}
