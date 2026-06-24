// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blobnode

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/desertbit/grumble"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdMigrateVerify(cmd *grumble.Command) {
	migrateVerifyCommand := &grumble.Command{
		Name: "migrate-verify",
		Help: "data integrity verification before/after node IP migration",
		LongHelp: "verify shards before and after node IP migration to ensure data integrity\n\n" +
			"sample: randomly sample shards on old host, record metadata to a manifest CSV\n" +
			"check:  inspect sampled shards on new host, compare CRC against the manifest",
	}
	cmd.AddCommand(migrateVerifyCommand)

	migrateVerifyCommand.AddCommand(&grumble.Command{
		Name: "sample",
		Help: "sample shards before migration (run on old host)",
		LongHelp: "randomly sample chunks and bids, read body CRC, and record to the manifest CSV\n\n" +
			"duration: max sampling time (0 = unlimited, e.g. 30m, 1h). " +
			"If sampling finishes earlier, the result is unchanged. " +
			"After the limit, each not-yet-started chunk still selected by chunk-rate records up to 100 shards.",
		Flags: func(f *grumble.Flags) {
			f.StringL("host", "", "blobnode HTTP address, e.g. http://10.0.0.1:8889")
			f.StringL("output", "", "manifest output path (CSV)")
			f.Float64L("chunk-rate", 0.10, "chunk sampling rate (0.0~1.0, default 0.10)")
			f.Float64L("bid-batch-rate", 0.10, "bid batch sampling rate per ListShards page (default 0.10)")
			f.Int64L("seed", 0, "deterministic random seed (0 = current timestamp)")
			f.DurationL("duration", 0, "max sampling time, 0 = unlimited (e.g. 30m, 1h); afterward each selected chunk samples up to 100 shards")
			f.IntL("disk-parallel", 0, "number of disks to process in parallel (0 = number of disks)")
			f.IntL("read-mbps-per-disk", 200, "per-disk read rate limit in MiB/s")
		},
		Run: func(c *grumble.Context) error {
			seed := c.Flags.Int64("seed")
			if seed == 0 {
				seed = time.Now().UnixNano()
			}
			return migrateVerifyRunSample(common.CmdContext(), migrateVerifySampleCfg{
				host:         c.Flags.String("host"),
				output:       c.Flags.String("output"),
				chunkRate:    c.Flags.Float64("chunk-rate"),
				bidBatchRate: c.Flags.Float64("bid-batch-rate"),
				seed:         seed,
				duration:     c.Flags.Duration("duration"),
				diskParallel: c.Flags.Int("disk-parallel"),
				readMBPS:     c.Flags.Int("read-mbps-per-disk"),
			})
		},
	})

	migrateVerifyCommand.AddCommand(&grumble.Command{
		Name:     "check",
		Help:     "verify shards after migration (run on new host)",
		LongHelp: "read shards from new host and compare CRC against the manifest CSV",
		Flags: func(f *grumble.Flags) {
			f.StringL("host", "", "blobnode HTTP address, e.g. http://10.0.0.2:8889")
			f.StringL("input", "", "manifest input path (CSV)")
			f.StringL("report", "", "report output path (JSON, optional)")
			f.DurationL("duration", 0, "max execution time, 0 = unlimited (e.g. 30m, 1h)")
			f.IntL("disk-parallel", 0, "number of disks to process in parallel (0 = number of disks)")
			f.IntL("read-mbps-per-disk", 200, "per-disk read rate limit in MiB/s")
		},
		Run: func(c *grumble.Context) error {
			return migrateVerifyRunCheck(common.CmdContext(), migrateVerifyCheckCfg{
				host:         c.Flags.String("host"),
				input:        c.Flags.String("input"),
				report:       c.Flags.String("report"),
				duration:     c.Flags.Duration("duration"),
				diskParallel: c.Flags.Int("disk-parallel"),
				readMBPS:     c.Flags.Int("read-mbps-per-disk"),
			})
		},
	})
}

// ---- constants ----

const (
	migrateVerifyBatchSize       = 100
	migrateVerifyManifestVersion = "1"
	migrateVerifyCSVHeader       = "disk_id,vuid,bid,size,crc,offset,flag,nopdata,inline"
)

// ---- config types ----

type migrateVerifySampleCfg struct {
	host         string
	output       string
	chunkRate    float64
	bidBatchRate float64
	seed         int64
	duration     time.Duration
	diskParallel int
	readMBPS     int
}

type migrateVerifyCheckCfg struct {
	host         string
	input        string
	report       string
	duration     time.Duration
	diskParallel int
	readMBPS     int
}

// ---- data types ----

// migrateVerifyEntry is one row in the manifest CSV: metadata for a sampled shard.
type migrateVerifyEntry struct {
	DiskID  proto.DiskID
	Vuid    proto.Vuid
	Bid     proto.BlobID
	Size    uint32 // logical shard size in bytes
	CRC     uint32 // IEEE CRC32 of body; for NopData/Inline: meta CRC from source
	Offset  int64  // physical offset in chunk file
	Flag    bnapi.ShardStatus
	NopData bool // all-zero shard; verified via StatShard (no body read)
	Inline  bool // data stored inline in metadata; verified via StatShard (no body read)
}

// migrateVerifyFailure is one item in the check report failures list.
type migrateVerifyFailure struct {
	DiskID        proto.DiskID `json:"disk_id"`
	Vuid          proto.Vuid   `json:"vuid"`
	Bid           proto.BlobID `json:"bid"`
	Reason        string       `json:"reason"` // "crc_mismatch" | "read_error"
	SourceMetaCRC uint32       `json:"source_meta_crc"`
	TargetMetaCRC uint32       `json:"target_meta_crc,omitempty"`
	TargetDataCRC uint32       `json:"target_data_crc,omitempty"`
	SourceSize    int64        `json:"source_size"`
	TargetSize    int64        `json:"target_size,omitempty"`
}

// migrateVerifySummary is the aggregate statistics in the check report.
type migrateVerifySummary struct {
	ManifestTotal            int            `json:"manifest_total"`
	Processed                int            `json:"processed"`
	Checked                  int            `json:"checked"`
	Pending                  int            `json:"pending"`
	OK                       int            `json:"ok"`
	SkippedBidDeleted        int            `json:"skipped_bid_deleted"`
	SkippedBidMigrated       int            `json:"skipped_bid_migrated"`
	Failed                   int            `json:"failed"`
	DiskNotNormal            int            `json:"disk_not_normal"`
	DiskNotNormalIDs         []proto.DiskID `json:"disk_not_normal_ids,omitempty"`
	DiskNotNormalBidEntries  int            `json:"disk_not_normal_bid_entries"`
	ChunkNotNormal           int            `json:"chunk_not_normal"`
	ChunkNotNormalVuids      []proto.Vuid   `json:"chunk_not_normal_vuids,omitempty"`
	ChunkNotNormalBidEntries int            `json:"chunk_not_normal_bid_entries"`
}

// migrateVerifyReport is the full check result written to --report.
type migrateVerifyReport struct {
	Input            string                 `json:"input"`
	NewHost          string                 `json:"new_host"`
	CheckTime        string                 `json:"check_time"`
	ElapsedSec       float64                `json:"elapsed_sec"`
	DurationLimitSec float64                `json:"duration_limit_sec,omitempty"`
	TimedOut         bool                   `json:"timed_out"`
	Summary          migrateVerifySummary   `json:"summary"`
	Failures         []migrateVerifyFailure `json:"failures"`
}

// migrateVerifyDiskResult is collected from each disk worker goroutine.
type migrateVerifyDiskResult struct {
	diskID                   proto.DiskID
	diskSkipped              bool // true = entire disk skipped at pre-check (not Normal)
	diskNotNormalBidEntries  int  // bid entries skipped due to disk not Normal / broken
	chunkNotNormalVuids      []proto.Vuid
	chunkNotNormalBidEntries int
	ok                       int
	skippedBidDeleted        int
	skippedBidMigrated       int
	failures                 []migrateVerifyFailure
}

// ---- utility helpers ----

func migrateVerifyResolveDiskParallel(flag, diskCount int) int {
	if flag <= 0 || flag > diskCount {
		return diskCount
	}
	return flag
}

func migrateVerifyNewRateLimiter(readMBPS int) *rate.Limiter {
	if readMBPS <= 0 {
		return nil
	}
	bps := readMBPS * 1024 * 1024
	return rate.NewLimiter(rate.Limit(bps), bps)
}

// migrateVerifyRateWait waits for n byte-tokens; handles n > burst by looping.
func migrateVerifyRateWait(ctx context.Context, lmt *rate.Limiter, n int64) error {
	if lmt == nil || n <= 0 {
		return ctx.Err()
	}
	burst := int64(lmt.Burst())
	for n > 0 {
		take := n
		if take > burst {
			take = burst
		}
		if err := lmt.WaitN(ctx, int(take)); err != nil {
			return err
		}
		n -= take
	}
	return nil
}

// migrateVerifyPickChunk returns true if the chunk should be sampled.
func migrateVerifyPickChunk(seed int64, diskID proto.DiskID, vuid proto.Vuid, samplingRate float64) bool {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(seed))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(diskID))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(vuid))
	h.Write(buf[:])
	return float64(h.Sum64()%10000) < samplingRate*10000
}

// migrateVerifyPickBatch returns true if the batch (a ListShards page) should be sampled.
func migrateVerifyPickBatch(seed int64, diskID proto.DiskID, vuid proto.Vuid, batchIdx int, samplingRate float64) bool {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(seed))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(diskID))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(vuid))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(batchIdx))
	h.Write(buf[:])
	return float64(h.Sum64()%10000) < samplingRate*10000
}

// migrateVerifyClassifyError classifies a blobnode error into skip categories.
func migrateVerifyClassifyError(err error) (deleted, migrated, diskNotNormal bool) {
	code := bloberr.DetectCode(err)
	switch code {
	case bloberr.CodeBidNotFound, bloberr.CodeShardMarkDeleted:
		deleted = true
	case bloberr.CodeVuidNotFound, bloberr.CodeDiskNotFound, bloberr.CodeVUIDRelease:
		migrated = true
	case bloberr.CodeDiskBroken:
		diskNotNormal = true
	}
	return
}

func migrateVerifyIntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---- manifest CSV read / write ----

func migrateVerifyEntryToRecord(e migrateVerifyEntry) []string {
	nop := "0"
	if e.NopData {
		nop = "1"
	}
	inl := "0"
	if e.Inline {
		inl = "1"
	}
	return []string{
		strconv.FormatUint(uint64(e.DiskID), 10),
		strconv.FormatUint(uint64(e.Vuid), 10),
		strconv.FormatUint(uint64(e.Bid), 10),
		strconv.FormatUint(uint64(e.Size), 10),
		strconv.FormatUint(uint64(e.CRC), 10),
		strconv.FormatInt(e.Offset, 10),
		strconv.FormatUint(uint64(e.Flag), 10),
		nop,
		inl,
	}
}

func migrateVerifyRecordToEntry(rec []string) (migrateVerifyEntry, error) {
	// Accept 9 columns (current: +inline) or 8 (legacy manifests without inline column).
	if len(rec) != 9 && len(rec) != 8 {
		return migrateVerifyEntry{}, fmt.Errorf("invalid CSV record length %d, want 9", len(rec))
	}
	diskID, err := strconv.ParseUint(rec[0], 10, 32)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("disk_id: %w", err)
	}
	vuid, err := strconv.ParseUint(rec[1], 10, 64)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("vuid: %w", err)
	}
	bid, err := strconv.ParseUint(rec[2], 10, 64)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("bid: %w", err)
	}
	size, err := strconv.ParseUint(rec[3], 10, 32)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("size: %w", err)
	}
	crc, err := strconv.ParseUint(rec[4], 10, 32)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("crc: %w", err)
	}
	offset, err := strconv.ParseInt(rec[5], 10, 64)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("offset: %w", err)
	}
	flag, err := strconv.ParseUint(rec[6], 10, 8)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("flag: %w", err)
	}
	nopdata, err := strconv.ParseUint(rec[7], 10, 8)
	if err != nil {
		return migrateVerifyEntry{}, fmt.Errorf("nopdata: %w", err)
	}
	var inline uint64
	if len(rec) == 9 {
		inline, err = strconv.ParseUint(rec[8], 10, 8)
		if err != nil {
			return migrateVerifyEntry{}, fmt.Errorf("inline: %w", err)
		}
	}
	return migrateVerifyEntry{
		DiskID:  proto.DiskID(diskID),
		Vuid:    proto.Vuid(vuid),
		Bid:     proto.BlobID(bid),
		Size:    uint32(size),
		CRC:     uint32(crc),
		Offset:  offset,
		Flag:    bnapi.ShardStatus(flag),
		NopData: nopdata == 1,
		Inline:  inline == 1,
	}, nil
}

// migrateVerifyReadManifest reads all sample entries from a manifest CSV file.
// Comment lines (starting with '#') and the header row are skipped.
func migrateVerifyReadManifest(path string) ([]migrateVerifyEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.Comment = '#'

	var entries []migrateVerifyEntry
	headerSkipped := false
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("CSV read error: %w", err)
		}
		if !headerSkipped {
			headerSkipped = true
			continue
		}
		e, err := migrateVerifyRecordToEntry(rec)
		if err != nil {
			return nil, fmt.Errorf("invalid manifest row %v: %w", rec, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// ========================
//       SAMPLE PHASE
// ========================

func migrateVerifyRunSample(ctx context.Context, cfg migrateVerifySampleCfg) error {
	if cfg.host == "" {
		return fmt.Errorf("--host is required")
	}
	if cfg.output == "" {
		return fmt.Errorf("--output is required")
	}
	if cfg.chunkRate < 0 || cfg.chunkRate > 1 {
		return fmt.Errorf("--chunk-rate must be between 0 and 1")
	}
	if cfg.bidBatchRate < 0 || cfg.bidBatchRate > 1 {
		return fmt.Errorf("--bid-batch-rate must be between 0 and 1")
	}
	if cfg.duration < 0 {
		return fmt.Errorf("--duration must be >= 0")
	}
	if cfg.diskParallel < 0 {
		return fmt.Errorf("--disk-parallel must be >= 0")
	}
	if cfg.readMBPS < 0 {
		return fmt.Errorf("--read-mbps-per-disk must be >= 0")
	}

	var deadline time.Time
	if cfg.duration > 0 {
		deadline = time.Now().Add(cfg.duration)
	}

	cli := bnapi.New(&bnapi.Config{})

	disks, err := cli.Stat(ctx, cfg.host)
	if err != nil {
		return fmt.Errorf("blobnode Stat failed: %w", err)
	}

	var normalDisks []*cmapi.BlobNodeDiskInfo
	for _, d := range disks {
		if d.Status == proto.DiskStatusNormal {
			normalDisks = append(normalDisks, d)
		}
	}
	if len(normalDisks) == 0 {
		return fmt.Errorf("no Normal disks found on %s", cfg.host)
	}

	parallel := migrateVerifyResolveDiskParallel(cfg.diskParallel, len(normalDisks))
	fmt.Printf("sample: host=%s disks=%d parallel=%d chunk-rate=%.2f bid-batch-rate=%.2f seed=%d duration=%s\n",
		cfg.host, len(normalDisks), parallel, cfg.chunkRate, cfg.bidBatchRate, cfg.seed, cfg.duration)

	// Per-disk temp file paths (disk-sorted order = final manifest order)
	sort.Slice(normalDisks, func(i, j int) bool { return normalDisks[i].DiskID < normalDisks[j].DiskID })
	tmpFiles := make([]string, len(normalDisks))
	for i, d := range normalDisks {
		tmpFiles[i] = fmt.Sprintf("%s.disk-%d.tmp", cfg.output, d.DiskID)
	}

	errs := make([]error, len(normalDisks))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i, disk := range normalDisks {
		wg.Add(1)
		go func(idx int, d *cmapi.BlobNodeDiskInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			errs[idx] = migrateVerifySampleDisk(ctx, cli, cfg, d.DiskID, tmpFiles[idx], deadline)
		}(i, disk)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			fmt.Printf("ERROR: disk %d sample error: %v\n", normalDisks[i].DiskID, e)
		}
	}

	// Merge disk temp files into the final manifest
	now := time.Now().UTC()
	if err := migrateVerifyMergeManifest(cfg, normalDisks, tmpFiles, now); err != nil {
		return fmt.Errorf("merge manifest: %w", err)
	}
	for _, tmp := range tmpFiles {
		os.Remove(tmp)
	}

	fmt.Printf("sample done: manifest written to %s\n", cfg.output)
	return nil
}

func migrateVerifySampleDisk(ctx context.Context, cli bnapi.StorageAPI, cfg migrateVerifySampleCfg,
	diskID proto.DiskID, tmpPath string, deadline time.Time,
) error {
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	w := csv.NewWriter(bw)

	lmt := migrateVerifyNewRateLimiter(cfg.readMBPS)

	chunks, err := cli.ListChunks(ctx, cfg.host, &bnapi.ListChunkArgs{DiskID: diskID})
	if err != nil {
		return fmt.Errorf("ListChunks disk %d: %w", diskID, err)
	}

	var eligibleChunks, sampledChunks, fastChunks, sampledEntries int
	fastAnnounced := false
	for _, chunk := range chunks {
		// Skip Released chunks and chunks being compacted (data may be in transition).
		if chunk.Status == cmapi.ChunkStatusRelease || chunk.Compacting {
			continue
		}
		eligibleChunks++
		// chunk-rate still decides which chunks are sampled after the deadline.
		// Selected chunks sample at most migrateVerifyBatchSize shards.
		if !migrateVerifyPickChunk(cfg.seed, diskID, chunk.Vuid, cfg.chunkRate) {
			continue
		}
		sampledChunks++

		fast := !deadline.IsZero() && time.Now().After(deadline)
		var (
			n   int
			err error
		)
		if fast {
			if !fastAnnounced {
				fastAnnounced = true
				fmt.Printf("disk %d: duration reached, remaining selected chunks sample up to %d shards each\n",
					diskID, migrateVerifyBatchSize)
			}
			fastChunks++
			n, err = migrateVerifySampleChunkLimited(ctx, cli, cfg, diskID, chunk.Vuid, lmt, w)
		} else {
			n, err = migrateVerifySampleChunk(ctx, cli, cfg, diskID, chunk.Vuid, lmt, w, deadline)
		}
		if err != nil {
			fmt.Printf("ERROR: disk %d vuid %d sample error: %v\n", diskID, chunk.Vuid, err)
		}
		sampledEntries += n
	}

	w.Flush()
	if err := bw.Flush(); err != nil {
		return err
	}
	hint := ""
	if sampledChunks > 0 && sampledEntries == 0 {
		hint = " (hint: chunks may be sparse; try increasing --bid-batch-rate or re-running with a different --seed)"
	}
	fmt.Printf("disk %d: total_chunks=%d sampled_chunks=%d fast_chunks=%d sampled_entries=%d%s\n",
		diskID, eligibleChunks, sampledChunks, fastChunks, sampledEntries, hint)
	return nil
}

func migrateVerifySampleChunk(ctx context.Context, cli bnapi.StorageAPI, cfg migrateVerifySampleCfg,
	diskID proto.DiskID, vuid proto.Vuid, lmt *rate.Limiter, w *csv.Writer, deadline time.Time,
) (int, error) {
	var batchIdx int
	startBid := proto.InValidBlobID
	var total int

	for {
		// A chunk already in progress keeps the batches it has recorded, then stops
		// so one large chunk cannot run past the duration by a full scan.
		if batchIdx > 0 && !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		shards, next, err := cli.ListShards(ctx, cfg.host, &bnapi.ListShardsArgs{
			DiskID:   diskID,
			Vuid:     vuid,
			StartBid: startBid,
			Status:   bnapi.ShardStatusNormal,
			Count:    migrateVerifyBatchSize,
		})
		if err != nil {
			return total, fmt.Errorf("ListShards vuid=%d: %w", vuid, err)
		}

		if migrateVerifyPickBatch(cfg.seed, diskID, vuid, batchIdx, cfg.bidBatchRate) {
			n, err := migrateVerifySampleBatch(ctx, cli, cfg, diskID, vuid, shards, lmt, w)
			if err != nil {
				fmt.Printf("ERROR: disk=%d vuid=%d batch=%d: %v\n", diskID, vuid, batchIdx, err)
			}
			total += n
		}

		batchIdx++
		if next == proto.InValidBlobID {
			break
		}
		startBid = next
	}
	return total, nil
}

// migrateVerifySampleChunkLimited reads at most the first batch of a chunk.
// It is used after --duration and does not apply bid-batch-rate.
func migrateVerifySampleChunkLimited(ctx context.Context, cli bnapi.StorageAPI, cfg migrateVerifySampleCfg,
	diskID proto.DiskID, vuid proto.Vuid, lmt *rate.Limiter, w *csv.Writer,
) (int, error) {
	shards, _, err := cli.ListShards(ctx, cfg.host, &bnapi.ListShardsArgs{
		DiskID:   diskID,
		Vuid:     vuid,
		StartBid: proto.InValidBlobID,
		Status:   bnapi.ShardStatusNormal,
		Count:    migrateVerifyBatchSize,
	})
	if err != nil {
		return 0, fmt.Errorf("ListShards vuid=%d: %w", vuid, err)
	}
	if len(shards) == 0 {
		return 0, nil
	}
	return migrateVerifySampleBatch(ctx, cli, cfg, diskID, vuid, shards, lmt, w)
}

// migrateVerifySampleBatch reads one ListShards page, writes sampled entries to the CSV writer.
func migrateVerifySampleBatch(ctx context.Context, cli bnapi.StorageAPI, cfg migrateVerifySampleCfg,
	diskID proto.DiskID, vuid proto.Vuid, shards []*bnapi.ShardInfo,
	lmt *rate.Limiter, w *csv.Writer,
) (int, error) {
	var regularShards, nopShards, inlineShards []*bnapi.ShardInfo
	for _, s := range shards {
		if s.Flag == bnapi.ShardStatusMarkDelete {
			continue
		}
		switch {
		case s.NopData:
			nopShards = append(nopShards, s)
		case s.Inline || s.Size == 0:
			// Inline shards store data in metadata; zero-size shards have no body.
			// Neither can be read via GetShards (offsets not monotonically increasing
			// in the chunk file). Verify via StatShard meta-CRC comparison instead.
			inlineShards = append(inlineShards, s)
		default:
			regularShards = append(regularShards, s)
		}
	}

	var count int

	// NopData: record meta only, no body read
	for _, s := range nopShards {
		if err := w.Write(migrateVerifyEntryToRecord(migrateVerifyEntry{
			DiskID:  diskID,
			Vuid:    vuid,
			Bid:     s.Bid,
			Size:    uint32(s.Size),
			CRC:     s.Crc, // meta CRC
			Offset:  s.Offset,
			Flag:    s.Flag,
			NopData: true,
		})); err != nil {
			return count, err
		}
		count++
	}

	// Inline / zero-size: record meta only, no body read
	for _, s := range inlineShards {
		if err := w.Write(migrateVerifyEntryToRecord(migrateVerifyEntry{
			DiskID: diskID,
			Vuid:   vuid,
			Bid:    s.Bid,
			Size:   uint32(s.Size),
			CRC:    s.Crc, // meta CRC
			Offset: s.Offset,
			Flag:   s.Flag,
			Inline: true,
		})); err != nil {
			return count, err
		}
		count++
	}

	if len(regularShards) == 0 {
		return count, nil
	}

	// Sort by offset for HDD-friendly sequential read
	sort.Slice(regularShards, func(i, j int) bool {
		return regularShards[i].Offset < regularShards[j].Offset
	})

	bidInfos := make([]bnapi.BidInfo, len(regularShards))
	var totalBytes int64
	for i, s := range regularShards {
		bidInfos[i] = bnapi.BidInfo{Bid: s.Bid, Size: s.Size, Offset: s.Offset, Crc: s.Crc}
		totalBytes += s.Size
	}

	if err := migrateVerifyRateWait(ctx, lmt, totalBytes); err != nil {
		return count, ctx.Err()
	}

	getter, err := cli.GetShards(ctx, cfg.host, &bnapi.GetShardsArgs{
		DiskID: diskID,
		Vuid:   vuid,
		Bids:   bidInfos,
		Type:   bnapi.ReadIO,
	})
	if err != nil {
		return count, fmt.Errorf("GetShards: %w", err)
	}

	idx := 0
	for {
		body, nextErr, ok := getter.NextShard(ctx)
		if !ok {
			break
		}
		s := regularShards[idx]
		idx++

		if nextErr != nil {
			getter.Close()
			return count, fmt.Errorf("NextShard bid=%d: %w", s.Bid, nextErr)
		}

		h := crc32.NewIEEE()
		n, readErr := io.Copy(h, body)
		body.Close()

		if readErr != nil || n != s.Size {
			getter.Close()
			return count, fmt.Errorf("read body bid=%d: err=%v n=%d want=%d", s.Bid, readErr, n, s.Size)
		}

		localCRC := h.Sum32()
		if localCRC != s.Crc {
			fmt.Printf("ERROR: sample CRC mismatch disk=%d vuid=%d bid=%d meta_crc=%d body_crc=%d\n",
				diskID, vuid, s.Bid, s.Crc, localCRC)
		}

		// Store the locally computed body CRC as the verification basis
		if err := w.Write(migrateVerifyEntryToRecord(migrateVerifyEntry{
			DiskID:  diskID,
			Vuid:    vuid,
			Bid:     s.Bid,
			Size:    uint32(s.Size),
			CRC:     localCRC,
			Offset:  s.Offset,
			Flag:    s.Flag,
			NopData: false,
		})); err != nil {
			getter.Close()
			return count, err
		}
		count++
	}
	getter.Close()
	return count, nil
}

func migrateVerifyMergeManifest(cfg migrateVerifySampleCfg, _ []*cmapi.BlobNodeDiskInfo, tmpFiles []string, now time.Time) error {
	f, err := os.Create(cfg.output)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	fmt.Fprintf(bw,
		"# meta: version=%s node_host=%s sample_time=%s chunk_rate=%.2f bid_batch_rate=%.2f batch_size=%d seed=%d duration=%s tool=blobstore-cli\n",
		migrateVerifyManifestVersion, cfg.host, now.Format(time.RFC3339),
		cfg.chunkRate, cfg.bidBatchRate, migrateVerifyBatchSize, cfg.seed, cfg.duration)
	fmt.Fprintf(bw, "%s\n", migrateVerifyCSVHeader)

	for _, tmp := range tmpFiles {
		tf, err := os.Open(tmp)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		_, err = io.Copy(bw, bufio.NewReader(tf))
		tf.Close()
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ========================
//       CHECK PHASE
// ========================

func migrateVerifyRunCheck(ctx context.Context, cfg migrateVerifyCheckCfg) error {
	if cfg.host == "" {
		return fmt.Errorf("--host is required")
	}
	if cfg.input == "" {
		return fmt.Errorf("--input is required")
	}
	if cfg.duration < 0 {
		return fmt.Errorf("--duration must be >= 0")
	}
	if cfg.diskParallel < 0 {
		return fmt.Errorf("--disk-parallel must be >= 0")
	}
	if cfg.readMBPS < 0 {
		return fmt.Errorf("--read-mbps-per-disk must be >= 0")
	}

	startTime := time.Now()

	entries, err := migrateVerifyReadManifest(cfg.input)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	total := len(entries)
	fmt.Printf("check: host=%s manifest=%s entries=%d\n", cfg.host, cfg.input, total)

	// Apply --duration timeout
	checkCtx := ctx
	var cancelFn context.CancelFunc
	if cfg.duration > 0 {
		checkCtx, cancelFn = context.WithTimeout(ctx, cfg.duration)
		defer cancelFn()
	}

	cli := bnapi.New(&bnapi.Config{})

	// Pre-fetch disk status from new host
	diskInfos, err := cli.Stat(checkCtx, cfg.host)
	if err != nil {
		return fmt.Errorf("blobnode Stat: %w", err)
	}
	diskStatusMap := make(map[proto.DiskID]proto.DiskStatus, len(diskInfos))
	for _, d := range diskInfos {
		diskStatusMap[d.DiskID] = d.Status
	}

	// Group entries by disk_id
	byDisk := make(map[proto.DiskID][]migrateVerifyEntry)
	for _, e := range entries {
		byDisk[e.DiskID] = append(byDisk[e.DiskID], e)
	}
	diskIDs := make([]proto.DiskID, 0, len(byDisk))
	for id := range byDisk {
		diskIDs = append(diskIDs, id)
	}
	sort.Slice(diskIDs, func(i, j int) bool { return diskIDs[i] < diskIDs[j] })

	parallel := migrateVerifyResolveDiskParallel(cfg.diskParallel, len(diskIDs))
	fmt.Printf("checking %d disk(s) with parallelism %d\n", len(diskIDs), parallel)

	resultCh := make(chan migrateVerifyDiskResult, len(diskIDs))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, diskID := range diskIDs {
		wg.Add(1)
		go func(dID proto.DiskID) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			resultCh <- migrateVerifyCheckDisk(checkCtx, cli, cfg, dID, byDisk[dID], diskStatusMap)
		}(diskID)
	}
	wg.Wait()
	close(resultCh)

	elapsed := time.Since(startTime)
	timedOut := checkCtx.Err() != nil

	// Aggregate results
	sum := migrateVerifySummary{ManifestTotal: total}
	var failures []migrateVerifyFailure
	for res := range resultCh {
		if res.diskSkipped {
			sum.DiskNotNormal++
			sum.DiskNotNormalIDs = append(sum.DiskNotNormalIDs, res.diskID)
		}
		sum.DiskNotNormalBidEntries += res.diskNotNormalBidEntries
		sum.ChunkNotNormal += len(res.chunkNotNormalVuids)
		sum.ChunkNotNormalVuids = append(sum.ChunkNotNormalVuids, res.chunkNotNormalVuids...)
		sum.ChunkNotNormalBidEntries += res.chunkNotNormalBidEntries
		sum.OK += res.ok
		sum.SkippedBidDeleted += res.skippedBidDeleted
		sum.SkippedBidMigrated += res.skippedBidMigrated
		sum.Failed += len(res.failures)
		failures = append(failures, res.failures...)
	}
	sum.Checked = sum.OK + sum.SkippedBidDeleted + sum.SkippedBidMigrated + sum.Failed
	sum.Processed = sum.Checked + sum.DiskNotNormalBidEntries + sum.ChunkNotNormalBidEntries
	sum.Pending = total - sum.Processed
	if sum.Pending < 0 {
		sum.Pending = 0
	}

	if failures == nil {
		failures = []migrateVerifyFailure{}
	}
	report := migrateVerifyReport{
		Input:      cfg.input,
		NewHost:    cfg.host,
		CheckTime:  time.Now().UTC().Format(time.RFC3339),
		ElapsedSec: elapsed.Seconds(),
		TimedOut:   timedOut,
		Summary:    sum,
		Failures:   failures,
	}
	if cfg.duration > 0 {
		report.DurationLimitSec = cfg.duration.Seconds()
	}

	// Terminal summary
	suffix := ""
	if timedOut {
		suffix = " (timed out)"
	}
	fmt.Printf("\n=== migrate-verify check%s ===\n", suffix)
	fmt.Printf("duration: %s", elapsed.Round(time.Second))
	if cfg.duration > 0 {
		fmt.Printf(" (limit %s)", cfg.duration)
	}
	fmt.Println()
	fmt.Printf("manifest_total: %d  processed: %d  checked: %d  pending: %d\n",
		sum.ManifestTotal, sum.Processed, sum.Checked, sum.Pending)
	fmt.Printf("  ok: %d  skipped_bid_deleted: %d  skipped_bid_migrated: %d  failed: %d\n",
		sum.OK, sum.SkippedBidDeleted, sum.SkippedBidMigrated, sum.Failed)
	fmt.Printf("  disk_not_normal: %d disk(s)  chunk_not_normal: %d chunk(s)\n",
		sum.DiskNotNormal, sum.ChunkNotNormal)
	fmt.Printf("  disk_not_normal_bid_entries: %d  chunk_not_normal_bid_entries: %d\n",
		sum.DiskNotNormalBidEntries, sum.ChunkNotNormalBidEntries)

	if len(failures) > 0 {
		top := migrateVerifyIntMin(len(failures), 20)
		fmt.Printf("\nFAILURES (showing %d of %d):\n", top, len(failures))
		for i := 0; i < top; i++ {
			f := failures[i]
			fmt.Printf("  disk=%d vuid=%d bid=%d reason=%s src_crc=%d tgt_meta=%d tgt_data=%d\n",
				f.DiskID, f.Vuid, f.Bid, f.Reason, f.SourceMetaCRC, f.TargetMetaCRC, f.TargetDataCRC)
		}
	}

	if cfg.report != "" {
		data, _ := json.MarshalIndent(report, "", "  ")
		if werr := os.WriteFile(cfg.report, data, 0o644); werr != nil {
			fmt.Printf("WARN: write report %s: %v\n", cfg.report, werr)
		} else {
			fmt.Printf("report written: %s\n", cfg.report)
		}
	}

	if timedOut {
		fmt.Println("hint: increase --duration, or re-run after disks/chunks back to Normal")
	}

	// Return non-nil on actionable failures (exit code hint)
	if sum.Failed > 0 {
		return fmt.Errorf("check: %d entries have data integrity issues (see failures)", sum.Failed)
	}
	if timedOut || sum.Pending > 0 || sum.DiskNotNormal > 0 || sum.ChunkNotNormal > 0 {
		return fmt.Errorf("check incomplete: pending=%d disk_not_normal=%d chunk_not_normal=%d",
			sum.Pending, sum.DiskNotNormal, sum.ChunkNotNormal)
	}
	return nil
}

// migrateVerifyCheckDisk runs the pre-check + per-vuid verification for one disk.
func migrateVerifyCheckDisk(ctx context.Context, cli bnapi.StorageAPI, cfg migrateVerifyCheckCfg,
	diskID proto.DiskID, entries []migrateVerifyEntry, diskStatusMap map[proto.DiskID]proto.DiskStatus,
) migrateVerifyDiskResult {
	result := migrateVerifyDiskResult{diskID: diskID}

	// ---- disk-level pre-check ----
	status, exists := diskStatusMap[diskID]
	if !exists || status != proto.DiskStatusNormal {
		result.diskSkipped = true
		result.diskNotNormalBidEntries = len(entries)
		fmt.Printf("disk %d: status=%v, skipping %d entries\n", diskID, status, len(entries))
		return result
	}

	// ---- chunk-level pre-check ----
	chunks, err := cli.ListChunks(ctx, cfg.host, &bnapi.ListChunkArgs{DiskID: diskID})
	if err != nil {
		fmt.Printf("WARN: disk %d ListChunks error: %v; skipping disk\n", diskID, err)
		result.diskSkipped = true
		result.diskNotNormalBidEntries = len(entries)
		return result
	}
	chunkInfoMap := make(map[proto.Vuid]*cmapi.ChunkInfo, len(chunks))
	for _, c := range chunks {
		chunkInfoMap[c.Vuid] = c
	}

	// Group entries by vuid
	byVuid := make(map[proto.Vuid][]migrateVerifyEntry)
	for _, e := range entries {
		byVuid[e.Vuid] = append(byVuid[e.Vuid], e)
	}

	lmt := migrateVerifyNewRateLimiter(cfg.readMBPS)

	for vuid, vuidEntries := range byVuid {
		if ctx.Err() != nil {
			break
		}

		ci, exists := chunkInfoMap[vuid]
		if !exists {
			// chunk not present on this disk after migration
			result.skippedBidMigrated += len(vuidEntries)
			continue
		}
		// Skip if chunk is not Normal or is being compacted
		if ci.Status != cmapi.ChunkStatusNormal || ci.Compacting {
			result.chunkNotNormalVuids = append(result.chunkNotNormalVuids, vuid)
			result.chunkNotNormalBidEntries += len(vuidEntries)
			continue
		}

		// Separate by verification strategy:
		//   regular  → GetShards batch read + body CRC
		//   metaOnly → StatShard meta-CRC comparison (NopData and Inline entries)
		var regular, metaOnly []migrateVerifyEntry
		for _, e := range vuidEntries {
			if e.NopData || e.Inline {
				metaOnly = append(metaOnly, e)
			} else {
				regular = append(regular, e)
			}
		}

		// Sort regular entries by offset for sequential HDD reads
		sort.Slice(regular, func(i, j int) bool { return regular[i].Offset < regular[j].Offset })

		// Process in batches of migrateVerifyBatchSize via GetShards
		for start := 0; start < len(regular); start += migrateVerifyBatchSize {
			if ctx.Err() != nil {
				goto done
			}
			end := start + migrateVerifyBatchSize
			if end > len(regular) {
				end = len(regular)
			}
			migrateVerifyCheckBatch(ctx, cli, cfg.host, regular[start:end], lmt, &result)
		}

		// Handle NopData and Inline entries via StatShard (no body read needed)
		for i := range metaOnly {
			if ctx.Err() != nil {
				goto done
			}
			migrateVerifyNopData(ctx, cli, cfg.host, metaOnly[i], &result)
		}
	}

done:
	return result
}

// migrateVerifyCheckBatch verifies one batch (≤ migrateVerifyBatchSize) of regular (non-NopData) entries
// via GetShards; falls back to single GetShard when the stream errors.
func migrateVerifyCheckBatch(ctx context.Context, cli bnapi.StorageAPI, host string,
	entries []migrateVerifyEntry, lmt *rate.Limiter, result *migrateVerifyDiskResult,
) {
	if len(entries) == 0 {
		return
	}

	bidInfos := make([]bnapi.BidInfo, len(entries))
	var totalBytes int64
	for i, e := range entries {
		bidInfos[i] = bnapi.BidInfo{Bid: e.Bid, Size: int64(e.Size), Offset: e.Offset, Crc: e.CRC}
		totalBytes += int64(e.Size)
	}

	if err := migrateVerifyRateWait(ctx, lmt, totalBytes); err != nil {
		return // context canceled / timed out
	}

	getter, err := cli.GetShards(ctx, host, &bnapi.GetShardsArgs{
		DiskID: entries[0].DiskID,
		Vuid:   entries[0].Vuid,
		Bids:   bidInfos,
		Type:   bnapi.ReadIO,
	})
	if err != nil {
		// GetShards request-level failure: classify or fall back
		del, mig, dnorm := migrateVerifyClassifyError(err)
		if del {
			result.skippedBidDeleted += len(entries)
			return
		}
		if mig {
			result.skippedBidMigrated += len(entries)
			return
		}
		if dnorm {
			result.diskSkipped = true
			result.diskNotNormalBidEntries += len(entries)
			return
		}
		for _, e := range entries {
			migrateVerifySingle(ctx, cli, host, e, result)
		}
		return
	}

	var recheckQueue []migrateVerifyEntry
	idx := 0

	for {
		body, nextErr, ok := getter.NextShard(ctx)
		if !ok {
			break
		}
		e := entries[idx]
		idx++

		if nextErr != nil {
			// ErrBidNotMatch or stream-level error; stream is unrecoverable.
			// Most common cause: chunk was compacted between sample and check,
			// changing physical shard offsets. Fall back to single GetShard reads.
			getter.Close()
			migrateVerifySingle(ctx, cli, host, e, result)
			for i := idx; i < len(entries); i++ {
				migrateVerifySingle(ctx, cli, host, entries[i], result)
			}
			// Process any previously queued rechecks
			for _, re := range recheckQueue {
				migrateVerifyRecheck(ctx, cli, host, re, result)
			}
			return
		}

		h := crc32.NewIEEE()
		n, readErr := io.Copy(h, body)
		body.Close()

		if readErr != nil || n != int64(e.Size) {
			// Partial/errored read: stream position is unknown; close and fall back
			getter.Close()
			recheckQueue = append(recheckQueue, e)
			for i := idx; i < len(entries); i++ {
				migrateVerifySingle(ctx, cli, host, entries[i], result)
			}
			for _, re := range recheckQueue {
				migrateVerifyRecheck(ctx, cli, host, re, result)
			}
			return
		}

		localCRC := h.Sum32()
		if localCRC == e.CRC {
			result.ok++
		} else {
			recheckQueue = append(recheckQueue, e)
		}
	}
	getter.Close()

	for _, e := range recheckQueue {
		migrateVerifyRecheck(ctx, cli, host, e, result)
	}
}

// migrateVerifySingle verifies a single entry: NopData and Inline entries go through
// StatShard meta-CRC comparison; regular entries read the body via GetShard.
func migrateVerifySingle(ctx context.Context, cli bnapi.StorageAPI, host string,
	e migrateVerifyEntry, result *migrateVerifyDiskResult,
) {
	if e.NopData || e.Inline {
		migrateVerifyNopData(ctx, cli, host, e, result)
		return
	}

	localCRC, n, err := migrateVerifyReadBodyCRC(ctx, cli, host, e)
	if err != nil {
		del, mig, dnorm := migrateVerifyClassifyError(err)
		switch {
		case del:
			result.skippedBidDeleted++
		case mig:
			result.skippedBidMigrated++
		case dnorm:
			result.diskNotNormalBidEntries++
		default:
			migrateVerifyRecheck(ctx, cli, host, e, result)
		}
		return
	}

	if n == int64(e.Size) && localCRC == e.CRC {
		result.ok++
		return
	}
	migrateVerifyRecheck(ctx, cli, host, e, result)
}

// migrateVerifyNopData verifies a NopData or Inline entry by comparing meta (no body read).
func migrateVerifyNopData(ctx context.Context, cli bnapi.StorageAPI, host string,
	e migrateVerifyEntry, result *migrateVerifyDiskResult,
) {
	si, err := cli.StatShard(ctx, host, &bnapi.StatShardArgs{
		DiskID: e.DiskID,
		Vuid:   e.Vuid,
		Bid:    e.Bid,
	})
	if err != nil {
		del, mig, dnorm := migrateVerifyClassifyError(err)
		switch {
		case del:
			result.skippedBidDeleted++
		case mig:
			result.skippedBidMigrated++
		case dnorm:
			result.diskNotNormalBidEntries++
		default:
			result.failures = append(result.failures, migrateVerifyFailure{
				DiskID:        e.DiskID,
				Vuid:          e.Vuid,
				Bid:           e.Bid,
				Reason:        "read_error",
				SourceMetaCRC: e.CRC,
				SourceSize:    int64(e.Size),
			})
		}
		return
	}

	if uint32(si.Size) == e.Size && si.Crc == e.CRC && si.NopData == e.NopData {
		result.ok++
		return
	}
	result.failures = append(result.failures, migrateVerifyFailure{
		DiskID:        e.DiskID,
		Vuid:          e.Vuid,
		Bid:           e.Bid,
		Reason:        "crc_mismatch",
		SourceMetaCRC: e.CRC,
		TargetMetaCRC: si.Crc,
		SourceSize:    int64(e.Size),
		TargetSize:    si.Size,
	})
}

// migrateVerifyRecheck performs a second-pass verification on a CRC-mismatch or read-error entry.
// It compares source_meta_crc (manifest), target_meta_crc (StatShard), and
// target_data_crc (re-read body) to detect true corruption vs. transient issues.
func migrateVerifyRecheck(ctx context.Context, cli bnapi.StorageAPI, host string,
	e migrateVerifyEntry, result *migrateVerifyDiskResult,
) {
	// Step 1: re-read migration-target meta
	si, err := cli.StatShard(ctx, host, &bnapi.StatShardArgs{
		DiskID: e.DiskID,
		Vuid:   e.Vuid,
		Bid:    e.Bid,
	})
	if err != nil {
		del, mig, dnorm := migrateVerifyClassifyError(err)
		switch {
		case del:
			result.skippedBidDeleted++ // deleted between first read and recheck
		case mig:
			result.skippedBidMigrated++
		case dnorm:
			result.diskNotNormalBidEntries++
		default:
			result.failures = append(result.failures, migrateVerifyFailure{
				DiskID:        e.DiskID,
				Vuid:          e.Vuid,
				Bid:           e.Bid,
				Reason:        "read_error",
				SourceMetaCRC: e.CRC,
				SourceSize:    int64(e.Size),
			})
		}
		return
	}

	// Step 2: if NopData or Inline (either side), compare meta only
	if e.NopData || e.Inline || si.NopData {
		if uint32(si.Size) == e.Size && si.Crc == e.CRC && si.NopData == e.NopData {
			result.ok++ // meta matches; original discrepancy was transient
		} else {
			result.failures = append(result.failures, migrateVerifyFailure{
				DiskID:        e.DiskID,
				Vuid:          e.Vuid,
				Bid:           e.Bid,
				Reason:        "crc_mismatch",
				SourceMetaCRC: e.CRC,
				TargetMetaCRC: si.Crc,
				SourceSize:    int64(e.Size),
				TargetSize:    si.Size,
			})
		}
		return
	}

	// Step 3: re-read body and compute target_data_crc
	targetDataCRC, n, readErr := migrateVerifyReadBodyCRC(ctx, cli, host, e)
	if readErr != nil {
		result.failures = append(result.failures, migrateVerifyFailure{
			DiskID:        e.DiskID,
			Vuid:          e.Vuid,
			Bid:           e.Bid,
			Reason:        "read_error",
			SourceMetaCRC: e.CRC,
			TargetMetaCRC: si.Crc,
			SourceSize:    int64(e.Size),
			TargetSize:    si.Size,
		})
		return
	}

	if n != int64(e.Size) {
		result.failures = append(result.failures, migrateVerifyFailure{
			DiskID:        e.DiskID,
			Vuid:          e.Vuid,
			Bid:           e.Bid,
			Reason:        "crc_mismatch",
			SourceMetaCRC: e.CRC,
			TargetMetaCRC: si.Crc,
			TargetDataCRC: targetDataCRC,
			SourceSize:    int64(e.Size),
			TargetSize:    si.Size,
		})
		return
	}

	// source_meta_crc == target_meta_crc == target_data_crc → first read was a transient glitch
	if e.CRC == si.Crc && si.Crc == targetDataCRC {
		result.ok++
		return
	}

	// Any mismatch: confirmed data integrity failure
	result.failures = append(result.failures, migrateVerifyFailure{
		DiskID:        e.DiskID,
		Vuid:          e.Vuid,
		Bid:           e.Bid,
		Reason:        "crc_mismatch",
		SourceMetaCRC: e.CRC,
		TargetMetaCRC: si.Crc,
		TargetDataCRC: targetDataCRC,
		SourceSize:    int64(e.Size),
		TargetSize:    si.Size,
	})
}

// migrateVerifyReadBodyCRC reads the full shard body via GetShard and returns the IEEE CRC32.
// The body MUST be fully read so that the crc32block storage layer performs its
// per-block checksum, which surfaces silent data corruption.
func migrateVerifyReadBodyCRC(ctx context.Context, cli bnapi.StorageAPI, host string, e migrateVerifyEntry) (uint32, int64, error) {
	body, _, err := cli.GetShard(ctx, host, &bnapi.GetShardArgs{
		DiskID: e.DiskID,
		Vuid:   e.Vuid,
		Bid:    e.Bid,
		Type:   bnapi.ReadIO,
	})
	if err != nil {
		return 0, 0, err
	}
	defer body.Close()

	h := crc32.NewIEEE()
	n, err := io.Copy(h, body)
	return h.Sum32(), n, err
}
