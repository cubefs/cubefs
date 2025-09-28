package main

import (
	"bytes"
	"context"
	rand2 "crypto/rand"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/remotecache"
	"github.com/cubefs/cubefs/tool/remotecache-benchmark/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/bytespool"
	"github.com/google/uuid"
)

type BenchmarkTester struct {
	dataDir        string
	fileCache      sync.Map
	fileCounts     int64
	cacheStorage   storage.Storage
	nvmeStorage    storage.Storage
	hddStorage     storage.Storage
	verify         bool
	clearPageCache bool
}

type BenchmarkResult struct {
	StorageType   string
	OperationType string // "PUT" or "GET"
	TotalRequests int64
	SuccessCount  int64
	ErrorCount    int64
	TotalBytes    int64
	TotalTime     time.Duration
	Latencies     []time.Duration
	AvgLatency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
	Throughput    float64 // MB/s
	IOPS          float64
}

func NewBenchmarkTester(dataDir, hddBase, nvmeBase string, verify bool, master string, clearPageCache bool, logLevel string, disableBatch bool, activateTime int64, connWorkers int, flowLimit int64, writeChunkSize int) *BenchmarkTester {
	os.MkdirAll(dataDir, 0o755)
	tester := &BenchmarkTester{
		dataDir:        dataDir,
		cacheStorage:   nil, // TODO
		nvmeStorage:    nil,
		hddStorage:     nil,
		verify:         verify,
		clearPageCache: clearPageCache,
	}
	if hddBase != "" {
		fmt.Printf("\nCreate an HDD Tester %v\n", hddBase)
		os.MkdirAll(ensureAbsolutePath(hddBase), 0o755)
		tester.hddStorage = storage.NewLocalFS(ensureAbsolutePath(hddBase), dataDir, "hdd", verify)
	}
	if nvmeBase != "" {
		fmt.Printf("\nCreate a NVME Tester %v\n", nvmeBase)
		os.MkdirAll(ensureAbsolutePath(nvmeBase), 0o755)
		tester.nvmeStorage = storage.NewLocalFS(ensureAbsolutePath(nvmeBase), dataDir, "nvme", verify)
	}
	if master != "" {
		fmt.Printf("\nCreate a remote cache Tester %v\n", master)
		var err error
		cfg := &remotecache.ClientConfig{
			Masters:            strings.Split(master, ","),
			BlockSize:          proto.CACHE_OBJECT_BLOCK_SIZE,
			NeedInitLog:        true,
			LogLevelStr:        logLevel,
			LogDir:             "/tmp/cfs",
			ConnectTimeout:     500,
			FirstPacketTimeout: 1000,
			InitClientTime:     1,
			DisableBatch:       disableBatch,
			ActivateTime:       activateTime,
			ConnWorkers:        connWorkers,
			FlowLimit:          flowLimit,
			WriteChunkSize:     int64(writeChunkSize),
		}
		tester.cacheStorage, err = remotecache.NewRemoteCacheClient(cfg)
		if err != nil {
			fmt.Printf("\nCreate a remote cache Tester failed:%v\n", err)
		}
	}

	return tester
}

// TODO:Place 10,000 files in each folder.
func (t *BenchmarkTester) GenerateTestData(totalSizeGB, concurrency int) error {
	fmt.Printf("Concurrently generating %dGB of random files (0-4MB) in directory (%v)...\n", totalSizeGB, t.dataDir)
	var (
		generatedSize  int64  = 0
		fileCount      uint32 = 0
		totalSizeBytes        = int64(totalSizeGB) * util.GB
		wg             sync.WaitGroup
	)
	errorCh := make(chan error, concurrency)

	bufferPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 4*1024*1024) // Maximum 4MB
		},
	}

	generateFile := func() {
		defer wg.Done()
		buffer := bufferPool.Get().([]byte)
		defer bufferPool.Put(buffer) // nolint:staticcheck
		for {
			// Atomic operation to obtain the currently generated size and calculate the remaining space.
			currentSize := atomic.LoadInt64(&generatedSize)
			if currentSize >= totalSizeBytes {
				return
			}

			// Perform an atomic operation to get the currently generated size and calculate the remaining space.
			maxPossibleSize := totalSizeBytes - currentSize
			if maxPossibleSize <= 0 {
				return
			}

			if maxPossibleSize > 4*1024*1024 {
				maxPossibleSize = 4 * 1024 * 1024
			}

			baseSize := int64(1 * 1024 * 1024)
			maxRandom := maxPossibleSize - baseSize
			if maxRandom <= 0 {
				maxRandom = 1 // parameter for Int63n should always > 0
			}
			// Generate random file sizes
			fileSize := rand.Int63n(maxRandom) + baseSize

			if !atomic.CompareAndSwapInt64(&generatedSize, currentSize, currentSize+fileSize) {
				continue
			}

			key := fmt.Sprintf("file_%016x", rand.Uint64())
			filePath := filepath.Join(t.dataDir, key)

			if _, err := rand2.Read(buffer[:fileSize]); err != nil {
				errorCh <- fmt.Errorf("Failed to generate random data: %v", err)
				return
			}

			if err := os.WriteFile(filePath, buffer[:fileSize], 0o644); err != nil {
				errorCh <- fmt.Errorf("Failed to write to the file: %v", err)
				return
			}
			// Print the progress
			if atomic.AddUint32(&fileCount, 1)%1000 == 0 {
				fmt.Printf("\rGenerated: %d files, total size: %.2f GB",
					atomic.LoadUint32(&fileCount), float64(atomic.LoadInt64(&generatedSize))/(1024*1024*1024))
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go generateFile()
	}

	wg.Wait()
	fmt.Printf("\nData generation completed, with a total of %d files generated.\n", fileCount)

	select {
	case err := <-errorCh:
		return err
	default:
		return nil
	}
}

func ensureAbsolutePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Printf("Path resolution failed: %v, using the current directory.\n", err)
		return filepath.Clean(path)
	}
	return absPath
}

func main() {
	dataDir := flag.String("data", "./test_data", "Test data directory")
	totalSizeGB := flag.Int("size", 100, "Total size of test data (GB)")
	genConcurrency := flag.Int("gen-concurrency", util.Max(2, int(float64(runtime.NumCPU())*0.6)), "Number of concurrent data generation processes")
	concurrency := flag.Int("concurrency", util.Max(2, int(float64(runtime.NumCPU())*0.6)), "Number of concurrent requests")
	needGenerate := flag.Bool("need-generate", false, "Generate test data")
	hddBase := flag.String("hdd", "", "HDD storage directory")
	nvmeBase := flag.String("nvme", "", "NVME storage directory")
	master := flag.String("master", "", "Master address")
	runPutTest := flag.Bool("put-test", false, "Run the PUT test")
	runGetTest := flag.Bool("get-test", false, "Run the Get test")
	verify := flag.Bool("verify", true, "Verify with the source file")
	clear := flag.Bool("clear-cache", true, "Clear page cache")
	removeKey := flag.Bool("remove-key", false, "Remove specific key")
	blockKey := flag.String("block-key", "", "Block unique key")
	readRepeat := flag.Int("read-repeat", 1, "Read the repeat count of a key press")
	logLevel := flag.String("log-level", "debug", "Log level")
	disableBatch := flag.Bool("disable-batch", false, "Disable batch operations")
	activateTime := flag.Int64("activate-time", 200, "Activate time in microseconds")
	flashConnWorkers := flag.Int("flash-conn-workers", 64, "Number of flash connection workers")
	flowLimit := flag.Int64("flow-limit", 0, "Flow limit in bytes per second (default: 0 limiter is disable)")
	writeChunkSize := flag.Int("write-chunk-size", 64*1024, "Write chunk size in bytes (default: 64K)")
	flag.Parse()

	fmt.Printf("Parsed command-line arguments:\n")
	fmt.Printf("  -data: %s\n", *dataDir)
	fmt.Printf("  -size: %d\n", *totalSizeGB)
	fmt.Printf("  -gen-concurrency: %d\n", *genConcurrency)
	fmt.Printf("  -need-generate: %v\n", *needGenerate)
	fmt.Printf("  -hdd: %v\n", *hddBase)
	fmt.Printf("  -nvme: %v\n", *nvmeBase)
	fmt.Printf("  -put-test: %v\n", *runPutTest)
	fmt.Printf("  -get-test: %v\n", *runGetTest)
	fmt.Printf("  -concurrency: %v\n", *concurrency)
	fmt.Printf("  -verify: %v\n", *verify)
	fmt.Printf("  -master: %v\n", *master)
	fmt.Printf("  -clear-cache: %v\n", *clear)
	fmt.Printf("  -remove-key: %v\n", *removeKey)
	fmt.Printf("  -block-key: %v\n", *blockKey)
	fmt.Printf("  -read-repeat: %v\n", *readRepeat)
	fmt.Printf("  -log-level: %v\n", *logLevel)
	fmt.Printf("  -disable-batch: %v\n", *disableBatch)
	fmt.Printf("  -activate-time: %v\n", *activateTime)
	fmt.Printf("  -flash-conn-workers: %v\n", *flashConnWorkers)
	fmt.Printf("  -flow-limit: %v\n", *flowLimit)
	fmt.Printf("  -write-chunk-size: %v\n", *writeChunkSize)
	tester := NewBenchmarkTester(ensureAbsolutePath(*dataDir), *hddBase, *nvmeBase, *verify, *master, *clear, *logLevel, *disableBatch, *activateTime, *flashConnWorkers, *flowLimit, *writeChunkSize)
	if *needGenerate {
		if err := tester.GenerateTestData(*totalSizeGB, *genConcurrency); err != nil {
			fmt.Printf("generate test files failed: %v\n", err)
			return
		}
	} else {
		// only load file names
		if err := tester.LoadExistingTestFiles(*genConcurrency); err != nil {
			fmt.Printf("load test files failed: %v\n", err)
			return
		}
		fmt.Printf("load %v test files success\n", atomic.LoadInt64(&tester.fileCounts))
	}
	ctx := context.Background()
	if *runPutTest {
		// Clear all storage systems to ensure a clean test environment.
		fmt.Println("\nClearing all storage systems in preparation for the PUT test......")
		// In practical applications, it is necessary to implement the clearing logic.
		tester.RunPutBenchmark(ctx, *concurrency)
	}

	if *runGetTest {
		// Ensure all data has been preheated into the storage system.
		tester.RunGetBenchmark(ctx, *concurrency, *readRepeat)
	}

	if *removeKey && *blockKey != "" {
		tester.RunDeleteOperation(*blockKey)
	}
	tester.Stop()
}

func (t *BenchmarkTester) Stop() {
	if t.hddStorage != nil {
		t.hddStorage.Stop()
	}
	if t.nvmeStorage != nil {
		t.nvmeStorage.Stop()
	}

	if t.cacheStorage != nil {
		t.cacheStorage.Stop()
	}
}

func (t *BenchmarkTester) RunPutBenchmark(ctx context.Context, concurrency int) {
	fmt.Printf("\n=== Starting PUT performance test (concurrency: %d)===\n", concurrency)

	if t.hddStorage != nil {
		hddResult := t.runStoragePutBenchmark(ctx, t.hddStorage, concurrency)
		t.printBenchmarkResult(hddResult)
	}
	if t.nvmeStorage != nil {
		nvmeResult := t.runStoragePutBenchmark(ctx, t.nvmeStorage, concurrency)
		t.printBenchmarkResult(nvmeResult)
	}

	if t.cacheStorage != nil {
		remoteResult := t.runStoragePutBenchmark(ctx, t.cacheStorage, concurrency)
		t.printBenchmarkResult(remoteResult)
	}
}

func (t *BenchmarkTester) runStoragePutBenchmark(ctx context.Context, storage storage.Storage, concurrency int) *BenchmarkResult {
	fmt.Printf("Testing %s PUT performance...\n", storage.Name())
	result := &BenchmarkResult{
		StorageType:   storage.Name(),
		OperationType: "Put",
		Latencies:     make([]time.Duration, 0),
	}
	var (
		wg           sync.WaitGroup
		successCount int64
		errorCount   int64
		totalBytes   int64
		lastProgress = 0
		index        = 0
	)

	taskCh := make(chan string, 4*concurrency)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for k := range taskCh {
				value, ok := t.fileCache.Load(k)
				if !ok {
					atomic.AddInt64(&errorCount, 1)
					fmt.Printf("storage %v: %v not found\n", storage.Name(), k)
					continue
				}
				fh := value.(*FileHandler)
				startTime := time.Now()
				err := storage.Put(ctx, uuid.New().String(), fh.FileName, fh.Reader, fh.Size)
				latency := time.Since(startTime)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					fmt.Printf("storage %v put %v failed: %v\n", storage.Name(), fh.FileName, err)
				} else {
					atomic.AddInt64(&successCount, 1)
					atomic.AddInt64(&totalBytes, fh.Size)
					// Record the latency
					result.Latencies = append(result.Latencies, latency)
				}
				fh.Close()
			}
		}(w)
	}
	begin := time.Now()
	t.fileCache.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			fmt.Printf("context cancel storage %v put \n", storage.Name())
			return false
		case taskCh <- key.(string):
			// Print progress
			index++
			progress := index * 100 / int(t.fileCounts)
			if progress >= lastProgress+10 {
				lastProgress = progress
				fmt.Printf("\r%s Progress: %d%%", storage.Name(), progress)
			}
		}
		return true
	})
	close(taskCh)
	wg.Wait()
	totalTime := time.Since(begin)
	result.TotalRequests = atomic.LoadInt64(&t.fileCounts)
	result.SuccessCount = successCount
	result.ErrorCount = errorCount
	result.TotalBytes = totalBytes

	if len(result.Latencies) > 0 {
		sort.Slice(result.Latencies, func(i, j int) bool {
			return result.Latencies[i] < result.Latencies[j]
		})

		var totalLatency time.Duration
		for _, l := range result.Latencies {
			totalLatency += l
		}
		result.AvgLatency = totalLatency / time.Duration(len(result.Latencies))

		p95Idx := int(math.Min(float64(len(result.Latencies)-1), float64(len(result.Latencies))*0.95))
		p99Idx := int(math.Min(float64(len(result.Latencies)-1), float64(len(result.Latencies))*0.99))
		result.P95Latency = result.Latencies[p95Idx]
		result.P99Latency = result.Latencies[p99Idx]

		result.TotalTime = totalTime

		result.Throughput = float64(totalBytes) / (1024 * 1024) / result.TotalTime.Seconds()

		result.IOPS = float64(successCount) / result.TotalTime.Seconds()
	}
	return result
}

func (t *BenchmarkTester) LoadExistingTestFiles(concurrency int) error {
	fmt.Println("Loading existing test files...")
	absRoot, err := filepath.Abs(t.dataDir)
	if err != nil {
		fmt.Printf("Failed to obtain the absolute path.: %v\n", err)
		return err
	}
	if _, err := os.Stat(absRoot); os.IsNotExist(err) {
		fmt.Printf("Error: Directory %s does not exist.\n", absRoot)
		return err
	}
	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, concurrency)
	)

	err = filepath.WalkDir(absRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			sem <- struct{}{}
			wg.Add(1)
			go func(filePath string) {
				defer wg.Done()
				defer func() { <-sem }()
				fh, err := OpenFileAsReader(filePath)
				if err != nil {
					fmt.Printf("load %v failed:%v\n", filePath, err)
					return
				}
				t.fileCache.Store(filepath.Base(path), fh)
				atomic.AddInt64(&t.fileCounts, 1)
			}(path)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Walk dir error: %v\n", err)
		os.Exit(1)
	}

	wg.Wait()
	return nil
}

func (t *BenchmarkTester) RunDeleteOperation(blockKey string) {
	fmt.Printf("\n=== Starting Delete operation for key: %s ===\n", blockKey)
	if t.cacheStorage != nil {
		err := t.cacheStorage.Delete(context.Background(), blockKey)
		if err != nil {
			fmt.Printf("Delete operation for key: %s failed err %v \n", blockKey, err)
		}
	}
}

func (t *BenchmarkTester) printBenchmarkResult(result *BenchmarkResult) {
	if result == nil {
		fmt.Printf("\n--- no invalid BenchmarkResult ---\n")
		return
	}
	fmt.Printf("\n--- %s %sPerformance results ---\n", result.StorageType, result.OperationType)
	fmt.Printf("Total Requests: %d\n", result.TotalRequests)
	fmt.Printf("Successful Requests: %d\n", result.SuccessCount)
	fmt.Printf("Failed Requests: %d\n", result.ErrorCount)
	fmt.Printf("Total Data Volume: %.2f MB\n", float64(result.TotalBytes)/(1024*1024))
	fmt.Printf("Total Duration: %.2f seconds\n", result.TotalTime.Seconds())
	fmt.Printf("Average Latency: %.2f ms\n", result.AvgLatency.Seconds()*1000)
	fmt.Printf("P95 Latency: %.2f ms\n", result.P95Latency.Seconds()*1000)
	fmt.Printf("P99 Latency: %.2f ms\n", result.P99Latency.Seconds()*1000)
	fmt.Printf("Throughput: %.2f MB/s\n", result.Throughput)
	fmt.Printf("IOPS: %.2f\n", result.IOPS)
}

type FileHandler struct {
	Reader   io.Reader
	FileName string
	Size     int64
	bytes    []byte
}

func OpenFileAsReader(filePath string) (*FileHandler, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open File failed %v: %v", filePath, err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat File failed %v: %v", filePath, err)
	}
	buf := bytespool.Alloc(int(fileInfo.Size()))
	io.ReadFull(file, buf)

	info := &FileHandler{
		Reader:   bytes.NewReader(buf[0:fileInfo.Size()]),
		Size:     fileInfo.Size(),
		FileName: path.Base(filePath),
		bytes:    buf,
	}
	_ = file.Close()
	return info, nil
}

func (fh *FileHandler) Close() error {
	if fh.bytes != nil {
		bytespool.Free(fh.bytes)
		fh.bytes = nil
	}
	return nil
}

func (t *BenchmarkTester) RunGetBenchmark(ctx context.Context, concurrency int, repeats int) {
	fmt.Printf("\n=== Starting Get performance test (concurrency: %d) ===\n", concurrency)
	if t.hddStorage != nil {
		hddResult := t.runStorageGetBenchmark(ctx, t.hddStorage, concurrency, repeats)
		t.printBenchmarkResult(hddResult)
	}
	if t.nvmeStorage != nil {
		nvmeResult := t.runStorageGetBenchmark(ctx, t.nvmeStorage, concurrency, repeats)
		t.printBenchmarkResult(nvmeResult)
	}
	if t.cacheStorage != nil {
		remoteResult := t.runStorageGetBenchmark(ctx, t.cacheStorage, concurrency, repeats)
		t.printBenchmarkResult(remoteResult)
	}
}

func (t *BenchmarkTester) runStorageGetBenchmark(ctx context.Context, storage storage.Storage, concurrency int, repeats int) *BenchmarkResult {
	fmt.Printf("Testing %s Get performance...\n", storage.Name())
	// if t.clearPageCache {
	//	clearPageCache()
	// }

	result := &BenchmarkResult{
		StorageType:   storage.Name(),
		OperationType: "Get",
		Latencies:     make([]time.Duration, 0),
	}
	var (
		wg           sync.WaitGroup
		successCount int64
		errorCount   int64
		totalBytes   int64
		lastProgress = 0
		index        = 0
	)

	taskCh := make(chan string, 4*concurrency)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for k := range taskCh {
				value, ok := t.fileCache.Load(k)
				if !ok {
					atomic.AddInt64(&errorCount, 1)
					fmt.Printf("storage %v: %v not found\n", storage.Name(), k)
					continue
				}
				fh := value.(*FileHandler)
				from := int64(0)
				to := fh.Size
				var dataBuf, tmpBuf []byte
				dataBuf = bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE)
				tmpBuf = bytespool.Alloc(int(to - from))
				for i := 0; i < repeats; i++ {
					startTime := time.Now()
					reqId := uuid.New().String()
					r, len1, _, err := storage.Get(ctx, reqId, fh.FileName, from, to)
					var latency time.Duration
					if err == nil && r != nil {
						reads := 0
						for {
							readBytes, readErr := r.Read(dataBuf)
							if readErr != nil && readErr != io.EOF {
								fmt.Printf("storage %v read data %v failed reqID %v: readErr %v\n", storage.Name(), fh.FileName, reqId, readErr)
								err = readErr
								break
							}
							copy(tmpBuf[reads:], dataBuf[:readBytes])
							reads += readBytes
							if readErr == io.EOF {
								break
							}
						}
						if err == nil && reads != int(len1) {
							err = fmt.Errorf("wrong len %v:expected[%v]", reads, len1)
						} else if err == nil {
							latency = time.Since(startTime)
							if t.verify {
								readCrc := crc32.ChecksumIEEE(tmpBuf[:reads])
								actualCrc := crc32.ChecksumIEEE(fh.bytes[from:to])
								if actualCrc != readCrc {
									err = fmt.Errorf("wrong crc %v:expected[%v]", actualCrc, readCrc)
								}
							}
						}
					}
					if err != nil && err != io.EOF {
						atomic.AddInt64(&errorCount, 1)
						fmt.Printf("storage %v get %v failed reqID %v: %v\n", storage.Name(), fh.FileName, reqId, err)
					} else {
						atomic.AddInt64(&successCount, 1)
						atomic.AddInt64(&totalBytes, len1)
						result.Latencies = append(result.Latencies, latency)
					}
					if r != nil {
						r.Close()
					}
				}
				bytespool.Free(dataBuf)
				bytespool.Free(tmpBuf)
				fh.Close()
			}
		}(w)
	}
	begin := time.Now()
	t.fileCache.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			fmt.Printf("context cancel storage %v put \n", storage.Name())
			return false
		case taskCh <- key.(string):
			index++
			progress := index * 100 / int(t.fileCounts)
			if progress >= lastProgress+10 {
				lastProgress = progress
				fmt.Printf("\r%s Progress: %d%% \n", storage.Name(), progress)
			}
		}
		return true
	})
	close(taskCh)
	wg.Wait()
	totalTime := time.Since(begin)
	result.TotalRequests = atomic.LoadInt64(&t.fileCounts)
	result.SuccessCount = successCount
	result.ErrorCount = errorCount
	result.TotalBytes = totalBytes
	if len(result.Latencies) > 0 {
		sort.Slice(result.Latencies, func(i, j int) bool {
			return result.Latencies[i] < result.Latencies[j]
		})

		var totalLatency time.Duration
		for _, l := range result.Latencies {
			totalLatency += l
		}
		result.AvgLatency = totalLatency / time.Duration(len(result.Latencies))

		p95Idx := int(math.Min(float64(len(result.Latencies)-1), float64(len(result.Latencies))*0.95))
		p99Idx := int(math.Min(float64(len(result.Latencies)-1), float64(len(result.Latencies))*0.99))
		result.P95Latency = result.Latencies[p95Idx]
		result.P99Latency = result.Latencies[p99Idx]

		result.TotalTime = totalTime

		result.Throughput = float64(totalBytes) / (1024 * 1024) / result.TotalTime.Seconds()

		result.IOPS = float64(successCount) / result.TotalTime.Seconds()
	}
	return result
}

// func clearPageCache() {
//	data := []byte("1\n")
//	file, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0o644)
//	if err != nil {
//		fmt.Printf("open /proc/sys/vm/drop_caches failed: %v\n", err)
//		return
//	}
//	defer file.Close()
//
//	if _, err := file.Write(data); err != nil {
//		fmt.Printf("write to /proc/sys/vm/drop_caches failed: %v\n", err)
//	}
// }
