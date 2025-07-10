// ref: https://github.com/markhpc/hsbench/blob/master/hsbench.go
// Copyright (c) 2017 Wasabi Technology, Inc.
// Copyright (c) 2019 Red Hat Inc.
// Copyright (c) 2025 CMCC

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	bk "github.com/cubefs/cubefs/blobstore/tool/bench/backend"
	"github.com/cubefs/cubefs/blobstore/tool/bench/db"
)

// Global variables
var (
	backend, database, conf, modes, runName string
	objSize, outputFile, poolName           string
	maxObjCnt, maxErrCnt                    int64
	durationSecs, threads, loops            int
	interval                                float64
	size                                    int64
	objectData                              []byte
	dbDir                                   string
)

var (
	opCounter       int64
	objectCountFlag bool
	store           bk.ObjectStorage
	locdb           db.DB
)

func init() {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)
	flagSet.StringVar(&backend, "b", "blobstore", "Storage backend. 'blobstore' or 'dummy'")
	flagSet.StringVar(&conf, "c", os.Getenv("BLOBSTORE_BENCH_CONF"), "Conf file of blobstore")
	flagSet.StringVar(&modes, "m", "pgd", "Run modes in order.  See NOTES for more info")
	flagSet.StringVar(&runName, "pr", "", "Specifies the name of the test run, which also serves as a prefix for generated object names")
	flagSet.StringVar(&objSize, "s", "1Mi", "Size of objects in bytes with postfix Ki, Mi, and Gi")
	flagSet.StringVar(&database, "db", "", "Database mode. 'rocksdb' or 'memory'")
	flagSet.StringVar(&dbDir, "r", "", "RocksDB direcotry. Only for blobstore backend")

	flagSet.Int64Var(&maxObjCnt, "n", -1, "Maximum number of objects <-1 for unlimited>")
	flagSet.Int64Var(&maxErrCnt, "e", 3, "Maximum number of errors allowed <-1 for unlimited>")
	flagSet.IntVar(&durationSecs, "d", 60, "Maximum test duration in seconds <-1 for unlimited>")
	flagSet.IntVar(&threads, "t", 1, "Number of threads to run")
	flagSet.IntVar(&loops, "l", 1, "Number of times to repeat bench test")
	flag.Float64Var(&interval, "i", 1.0, "Number of seconds between report intervals")

	flag.StringVar(&outputFile, "o", "output.json", "Write JSON output to this file")

	notes := `
NOTES:
  - Valid mode types for the -m mode string are:
    p: put objects in blobstore
    g: get objects from blobstore
    d: delete objects from blobstore
`

	flagSet.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "\nUSAGE: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "OPTIONS:\n")
		flagSet.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "%s", notes)
	}

	var err error
	if err = flagSet.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	if maxObjCnt < 0 && durationSecs < 0 {
		log.Fatal("The number of objects and duration can not both be unlimited")
	}

	if conf == "" && backend != "dummy" {
		log.Fatal("Missing argument -c for storage cluster.")
	}

	for _, r := range modes {
		if r != 'p' && r != 'g' && r != 'd' {
			log.Fatal("Invalid modes passed to -m, see help for details.")
		}
	}

	if ((strings.Contains(modes, "g") || strings.Contains(modes, "d")) &&
		!strings.Contains(modes, "p")) && maxObjCnt < 0 {
		log.Fatal("The number of objects(-n XXX) must be specified for g(get)/d(del) test")
	}

	if size, err = parseSize(objSize); err != nil {
		log.Fatalf("Invalid -s argument for object size: %v", err)
	}

	if backend == "blobstore" && database == "rocksdb" && dbDir == "" {
		log.Fatal("-r must be specified for blobstore backend")
	}

	if maxErrCnt == -1 {
		maxErrCnt = math.MaxInt64
	}

	prepareData()
}

func parseSize(sizeStr string) (int64, error) {
	size, err := humanize.ParseBytes(sizeStr)
	if err != nil {
		return 0, err
	}
	if size > 1<<30 {
		return 0, fmt.Errorf("size too large >1G")
	}
	return int64(size), nil
}

func prepareData() {
	objectData = make([]byte, size)
	rand.Read(objectData)
	hasher := md5.New()
	hasher.Write(objectData)
	objectDataMD5 := hex.EncodeToString(hasher.Sum(nil))
	log.Printf("DataSize=%d MD5=%s", size, objectDataMD5)
}

// print all paramgs
func printParams() {
	log.Println("Parameters:")
	log.Printf("backend=%s", backend)
	log.Printf("database=%s", database)
	log.Printf("conf=%s", conf)
	log.Printf("runName=%s", runName)
	log.Printf("poolName=%s", poolName)
	log.Printf("objSize=%s", objSize)
	log.Printf("maxObjCnt=%d", maxObjCnt)
	log.Printf("durationSecs=%d", durationSecs)
	log.Printf("threads=%d", threads)
	log.Printf("loops=%d", loops)
	log.Printf("interval=%f", interval)
	log.Printf("dbDir=%s", dbDir)
}

type OutputStats struct {
	Loop          int
	IntervalName  string
	Seconds       float64
	Mode          string
	Ops           int
	Mbps          float64
	Iops          float64
	MinLat        float64
	AvgLat        float64
	NinetyNineLat float64
	MaxLat        float64
	Slowdowns     int64
}

func (o *OutputStats) log() {
	log.Printf("Loop: %d, Int: %s, Dur(s): %.1f, Mode: %s, Ops: %d, MB/s: %.2f, IO/s: %.0f, "+
		"Lat(ms): [min: %.1f, avg: %.1f, 99%%: %.1f, max: %.1f], Slowdowns: %d",
		o.Loop, o.IntervalName, o.Seconds, o.Mode, o.Ops, o.Mbps, o.Iops,
		o.MinLat, o.AvgLat, o.NinetyNineLat, o.MaxLat, o.Slowdowns)
}

type IntervalStats struct {
	loop         int
	name         string
	mode         string
	bytes        int64
	slowdowns    int64
	intervalNano int64
	latNano      []int64
}

func (is *IntervalStats) makeOutputStats() OutputStats {
	// Compute and log the stats
	ops := len(is.latNano)
	totalLat := int64(0)
	minLat := float64(0)
	maxLat := float64(0)
	NinetyNineLat := float64(0)
	avgLat := float64(0)
	if ops > 0 {
		minLat = float64(is.latNano[0]) / 1000000
		maxLat = float64(is.latNano[ops-1]) / 1000000
		for i := range is.latNano {
			totalLat += is.latNano[i]
		}
		avgLat = float64(totalLat) / float64(ops) / 1000000
		NintyNineLatNano := is.latNano[int64(math.Round(0.99*float64(ops)))-1]
		NinetyNineLat = float64(NintyNineLatNano) / 1000000
	}
	seconds := float64(is.intervalNano) / 1000000000
	mbps := float64(is.bytes) / seconds / (1 << 20)
	iops := float64(ops) / seconds

	return OutputStats{
		is.loop, is.name, seconds, is.mode, ops, mbps, iops,
		minLat, avgLat, NinetyNineLat, maxLat, is.slowdowns,
	}
}

type ThreadStats struct {
	start       int64
	curInterval int64
	intervals   []IntervalStats
}

func makeThreadStats(s int64, loop int, mode string, intervalNano int64) ThreadStats {
	ts := ThreadStats{s, 0, []IntervalStats{}}
	ts.intervals = append(ts.intervals, IntervalStats{loop, "0", mode, 0, 0, intervalNano, []int64{}})
	return ts
}

func (ts *ThreadStats) updateIntervals(loop int, mode string, intervalNano int64) int64 {
	// Interval statistics disabled, so just return the current interval
	if intervalNano < 0 {
		return ts.curInterval
	}
	for ts.start+intervalNano*(ts.curInterval+1) < time.Now().UnixNano() {
		ts.curInterval++
		ts.intervals = append(ts.intervals,
			IntervalStats{loop, strconv.FormatInt(ts.curInterval, 10), mode, 0, 0, intervalNano, []int64{}})
	}
	return ts.curInterval
}

func (ts *ThreadStats) finish() {
	ts.curInterval = -1
}

type Stats struct {
	// threads
	threads int
	// The loop we are in
	loop int
	// Test mode being run
	mode string
	// start time in nanoseconds
	startNano int64
	// end time in nanoseconds
	endNano int64
	// Duration in nanoseconds for each interval
	intervalNano int64
	// Per-thread statistics
	threadStats []ThreadStats
	// a map of per-interval thread completion counters
	intervalCompletions sync.Map
	// a counter of how many threads have finished updating stats entirely
	completions int32
}

func makeStats(loop int, mode string, threads int, intervalNano int64) *Stats {
	start := time.Now().UnixNano()
	s := &Stats{threads, loop, mode, start, 0, intervalNano, []ThreadStats{}, sync.Map{}, 0}
	for i := 0; i < threads; i++ {
		s.threadStats = append(s.threadStats, makeThreadStats(start, s.loop, s.mode, s.intervalNano))
		s.updateIntervals(i)
	}
	return s
}

func (stats *Stats) makeOutputStats(i int64) (OutputStats, bool) {
	// Check bounds first
	if stats.intervalNano < 0 || i < 0 {
		return OutputStats{}, false
	}
	// Not safe to log if not all writers have completed.
	value, ok := stats.intervalCompletions.Load(i)
	if !ok {
		return OutputStats{}, false
	}
	cp, ok := value.(*int32)
	if !ok {
		return OutputStats{}, false
	}
	count := atomic.LoadInt32(cp)
	if count < int32(stats.threads) {
		return OutputStats{}, false
	}

	bytes := int64(0)
	ops := int64(0)
	slowdowns := int64(0)

	for t := 0; t < stats.threads; t++ {
		bytes += stats.threadStats[t].intervals[i].bytes
		ops += int64(len(stats.threadStats[t].intervals[i].latNano))
		slowdowns += stats.threadStats[t].intervals[i].slowdowns
	}
	// Aggregate the per-thread Latency slice
	tmpLat := make([]int64, ops)
	var c int
	for t := 0; t < stats.threads; t++ {
		c += copy(tmpLat[c:], stats.threadStats[t].intervals[i].latNano)
	}
	sort.Slice(tmpLat, func(i, j int) bool { return tmpLat[i] < tmpLat[j] })
	is := IntervalStats{stats.loop, strconv.FormatInt(i, 10), stats.mode, bytes, slowdowns, stats.intervalNano, tmpLat}
	return is.makeOutputStats(), true
}

// Only safe to call from the calling thread
func (stats *Stats) updateIntervals(threadNum int) int64 {
	curInterval := stats.threadStats[threadNum].curInterval
	newInterval := stats.threadStats[threadNum].updateIntervals(stats.loop, stats.mode, stats.intervalNano)

	// Finish has already been called
	if curInterval < 0 {
		return -1
	}

	for i := curInterval; i < newInterval; i++ {
		// load or store the current value
		value, _ := stats.intervalCompletions.LoadOrStore(i, new(int32))
		cp, ok := value.(*int32)
		if !ok {
			log.Printf("updateIntervals: got data of type %T but wanted *int32", value)
			continue
		}

		count := atomic.AddInt32(cp, 1)
		if count == int32(stats.threads) {
			if is, ok := stats.makeOutputStats(i); ok {
				is.log()
			}
		}
	}
	return newInterval
}

func (stats *Stats) makeTotalStats() (OutputStats, bool) {
	// Not safe to log if not all writers have completed.
	completions := atomic.LoadInt32(&stats.completions)
	if completions < int32(threads) {
		log.Printf("log, completions: %d", completions)
		return OutputStats{}, false
	}

	bytes := int64(0)
	ops := int64(0)
	slowdowns := int64(0)

	for t := 0; t < stats.threads; t++ {
		for i := 0; i < len(stats.threadStats[t].intervals); i++ {
			bytes += stats.threadStats[t].intervals[i].bytes
			ops += int64(len(stats.threadStats[t].intervals[i].latNano))
			slowdowns += stats.threadStats[t].intervals[i].slowdowns
		}
	}
	// Aggregate the per-thread Latency slice
	tmpLat := make([]int64, ops)
	var c int
	for t := 0; t < stats.threads; t++ {
		for i := 0; i < len(stats.threadStats[t].intervals); i++ {
			c += copy(tmpLat[c:], stats.threadStats[t].intervals[i].latNano)
		}
	}
	sort.Slice(tmpLat, func(i, j int) bool { return tmpLat[i] < tmpLat[j] })
	is := IntervalStats{stats.loop, "TOTAL", stats.mode, bytes, slowdowns, stats.endNano - stats.startNano, tmpLat}
	return is.makeOutputStats(), true
}

func (stats *Stats) addOp(threadNum int, bytes int64, latNano int64) {
	// Interval statistics
	cur := stats.threadStats[threadNum].curInterval
	if cur < 0 {
		return
	}
	stats.threadStats[threadNum].intervals[cur].bytes += bytes
	stats.threadStats[threadNum].intervals[cur].latNano = append(stats.threadStats[threadNum].intervals[cur].latNano, latNano)
}

func (stats *Stats) addSlowDown(threadNum int) {
	cur := stats.threadStats[threadNum].curInterval
	stats.threadStats[threadNum].intervals[cur].slowdowns++
}

func (stats *Stats) finish(threadNum int) {
	stats.updateIntervals(threadNum)
	stats.threadStats[threadNum].finish()
	count := atomic.AddInt32(&stats.completions, 1)
	if count == int32(stats.threads) {
		stats.endNano = time.Now().UnixNano()
	}
}

func formatKeyString(objnum int64) string {
	return fmt.Sprintf("%s%012d", runName, objnum)
}

// kickoff put load
func runPutLoad(ctx context.Context, threadNum int, fendtime time.Time, stats *Stats) {
	errcnt := int64(0)
	for {
		if durationSecs > -1 && time.Now().After(fendtime) {
			break
		}
		objnum := atomic.AddInt64(&opCounter, 1)
		if maxObjCnt > -1 && objnum >= maxObjCnt {
			atomic.AddInt64(&opCounter, -1)
			break
		}

		fileobj := bytes.NewReader(objectData)
		var err error

		start := time.Now().UnixNano()
		loc, err := store.PutObject(ctx, fileobj, fileobj.Size())
		if err != nil {
			stats.addSlowDown(threadNum)
			atomic.AddInt64(&opCounter, -1)
			log.Printf("upload err: %v", err)
			if errcnt++; errcnt >= maxErrCnt {
				break
			}
			continue
		}

		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if backend == "blobstore" {
			key := formatKeyString(objnum)
			location, _ := loc.ExtractBlobLocation()
			if err = locdb.Put(key, location); err != nil {
				stats.addSlowDown(threadNum)
				atomic.AddInt64(&opCounter, -1)
				log.Printf("upload err: %v", err)
				if errcnt++; errcnt >= maxErrCnt {
					break
				}
			}
		}
		if err == nil {
			// Update the stats
			stats.addOp(threadNum, size, end-start)
		}
	}
	stats.finish(threadNum)
}

// kickoff get load
func runGetLoad(ctx context.Context, threadNum int, fendtime time.Time, stats *Stats) {
	errcnt := int64(0)
	for {
		if durationSecs > -1 && time.Now().After(fendtime) {
			break
		}
		objnum := atomic.AddInt64(&opCounter, 1)
		if maxObjCnt > -1 && objnum >= maxObjCnt {
			atomic.AddInt64(&opCounter, -1)
			break
		}

		// get key/location info from location store
		var err error
		var loc bk.LocInfo
		key := formatKeyString(objnum)
		if backend == "blobstore" {
			loc.Value, err = locdb.Get(key)
			if err != nil {
				errcnt++
				stats.addSlowDown(threadNum)
				log.Printf("download err %v", err)
				continue
			}
		}

		start := time.Now().UnixNano()
		writer := io.Discard // Use io.Discard to discard the data
		err = store.GetObject(ctx, loc, writer, size)
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			errcnt++
			stats.addSlowDown(threadNum)
			log.Printf("download err %v", err)
		} else {
			// Update the stats
			stats.addOp(threadNum, size, end-start)
		}
		if errcnt >= maxErrCnt {
			break
		}
	}
	stats.finish(threadNum)
}

// kickoff del load
func runDelLoad(ctx context.Context, threadNum int, fendtime time.Time, stats *Stats) {
	errcnt := int64(0)
	for {
		if durationSecs > -1 && time.Now().After(fendtime) {
			break
		}
		objnum := atomic.AddInt64(&opCounter, 1)
		if maxObjCnt > -1 && objnum >= maxObjCnt {
			atomic.AddInt64(&opCounter, -1)
			break
		}

		// get key/location info from location store
		var err error
		var loc bk.LocInfo

		key := formatKeyString(objnum)
		if backend == "blobstore" {
			loc.Value, err = locdb.Get(key)
			if err != nil {
				errcnt++
				stats.addSlowDown(threadNum)
				log.Printf("download err %v", err)
				continue
			}
		}

		start := time.Now().UnixNano()
		err = store.DelObject(ctx, loc)
		end := time.Now().UnixNano()
		stats.updateIntervals(threadNum)

		if err != nil {
			errcnt++
			stats.addSlowDown(threadNum)
			log.Printf("delete err %v", err)
		} else {
			// delete key/location info. Ignore error.
			if backend == "blobstore" {
				locdb.Del(key)
			}
			// Update the stats
			stats.addOp(threadNum, size, end-start)
		}
		if errcnt >= maxErrCnt {
			break
		}
	}
	stats.finish(threadNum)
}

func runWrapper(ctx context.Context, loop int, r rune) []OutputStats {
	atomic.StoreInt64(&opCounter, -1)
	intervalNano := int64(interval * 1000000000)
	endtime := time.Now().Add(time.Second * time.Duration(durationSecs))

	// If we perviously set the object count after running a put
	// test, set the object count back to -1 for the new put test.
	if r == 'p' && objectCountFlag {
		maxObjCnt = -1
		objectCountFlag = false
	}

	var stats *Stats
	var wg sync.WaitGroup
	switch r {
	case 'p':
		log.Printf("Running Loop %d Put Test", loop)
		stats = makeStats(loop, "PUT", threads, intervalNano)
		wg.Add(threads)
		for n := 0; n < threads; n++ {
			go func(n int) {
				defer wg.Done()
				runPutLoad(ctx, n, endtime, stats)
			}(n)
		}
	case 'g':
		log.Printf("Running Loop %d Get Test", loop)
		stats = makeStats(loop, "GET", threads, intervalNano)
		wg.Add(threads)
		for n := 0; n < threads; n++ {
			go func(n int) {
				defer wg.Done()
				runGetLoad(ctx, n, endtime, stats)
			}(n)
		}
	case 'd':
		log.Printf("Running Loop %d Del Test", loop)
		stats = makeStats(loop, "DEL", threads, intervalNano)
		wg.Add(threads)
		for n := 0; n < threads; n++ {
			go func(n int) {
				defer wg.Done()
				runDelLoad(ctx, n, endtime, stats)
			}(n)
		}
	}
	wg.Wait()

	// If the user didn't set the object_count, we can set it here
	// to limit subsequent get/del tests to valid objects only.
	if r == 'p' && maxObjCnt < 0 {
		maxObjCnt = atomic.LoadInt64(&opCounter) + 1
		objectCountFlag = true
	}

	// Create the Output Stats
	os := make([]OutputStats, 0)
	for i := int64(0); i >= 0; i++ {
		if o, ok := stats.makeOutputStats(i); ok {
			os = append(os, o)
		} else {
			break
		}
	}
	if o, ok := stats.makeTotalStats(); ok {
		o.log()
		os = append(os, o)
		if r == 'p' {
			log.Printf("The subsequent '-m g' or '-m d' test should be specified with '-n %d'", o.Ops)
		}
	}

	return os
}

func main() {
	log.Printf("BlobStore Benchmark")

	printParams()

	// init storage
	var err error
	switch backend {
	case "blobstore":
		store, err = bk.NewBlobStorage(conf)
	case "dummy":
		store, err = bk.NewDummyStorage()
	default:
		err = fmt.Errorf("backend %s not supported", backend)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	switch database {
	case "rocksdb":
		locdb, err = db.NewRocksDB(dbDir)
	default:
		locdb, err = db.NewMemoryDB()
	}
	if err != nil {
		log.Fatal(err)
	}

	// loop running the tests
	ctx := context.Background()
	oStats := make([]OutputStats, 0)
	for loop := 0; loop < loops; loop++ {
		for _, r := range modes {
			oStats = append(oStats, runWrapper(ctx, loop+1, r)...)
		}
	}

	if outputFile == "" {
		return
	}

	// write test result to specified file
	file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY, 0o777)
	if err != nil {
		log.Fatal("Could not open JSON file for writing.")
	}
	defer file.Close()

	data, err := json.Marshal(oStats)
	if err != nil {
		log.Fatal("Error marshaling JSON: ", err)
	}
	_, err = file.Write(data)
	if err != nil {
		log.Fatal("Error writing to JSON file: ", err)
	}
	file.Sync()
}
