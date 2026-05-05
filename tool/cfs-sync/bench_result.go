package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type benchResult struct {
	Mode          string  `json:"mode"`
	BlockSize     int     `json:"block_size"`
	NumJobs       int     `json:"num_jobs"`
	RuntimeSec    int     `json:"runtime_sec"`
	TotalBytes    int64   `json:"total_bytes"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	ThroughputMBs float64 `json:"throughput_mb_s"`
	IOPS          float64 `json:"iops"`
	ErrorCount    int64   `json:"error_count"`
}

func buildResult(mode string, bs, numjobs, runtime int, totalBytes int64, elapsed time.Duration, errCount int64) benchResult {
	sec := elapsed.Seconds()
	mbps := float64(totalBytes) / 1024 / 1024 / sec
	iops := float64(totalBytes) / float64(bs) / sec
	return benchResult{
		Mode:          mode,
		BlockSize:     bs,
		NumJobs:       numjobs,
		RuntimeSec:    runtime,
		TotalBytes:    totalBytes,
		ElapsedSec:    sec,
		ThroughputMBs: mbps,
		IOPS:          iops,
		ErrorCount:    errCount,
	}
}

func printThroughput(op string, bytes int64, elapsed time.Duration) {
	sec := elapsed.Seconds()
	if sec <= 0 {
		return
	}
	mbps := float64(bytes) / 1024 / 1024 / sec
	fmt.Printf("%s: %s in %.2f s → %.2f MB/s\n", op, humanBytes(bytes), sec, mbps)
}

func printResult(r benchResult, format string) {
	if format == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(r)
		return
	}
	fmt.Printf("mode       : %s\n", r.Mode)
	fmt.Printf("block_size : %s\n", humanBytes(int64(r.BlockSize)))
	fmt.Printf("num_jobs   : %d\n", r.NumJobs)
	fmt.Printf("elapsed    : %.2f s\n", r.ElapsedSec)
	fmt.Printf("total      : %s\n", humanBytes(r.TotalBytes))
	fmt.Printf("throughput : %.2f MB/s\n", r.ThroughputMBs)
	fmt.Printf("iops       : %.1f\n", r.IOPS)
	if r.ErrorCount > 0 {
		fmt.Printf("errors     : %d\n", r.ErrorCount)
	}
}

func printFlashResult(r benchResult, format string,
	writeBytes, readBytes int64,
	writeElapsed, readElapsed time.Duration,
	writeErrs, readErrs int64,
) {
	if format == "json" {
		out := map[string]interface{}{
			"mode":       r.Mode,
			"block_size": r.BlockSize,
			"num_jobs":   r.NumJobs,
		}
		if writeElapsed > 0 {
			sec := writeElapsed.Seconds()
			out["write_bytes"] = writeBytes
			out["write_elapsed_sec"] = sec
			out["write_throughput_mb_s"] = float64(writeBytes) / 1024 / 1024 / sec
			out["write_errors"] = writeErrs
		}
		if readElapsed > 0 {
			sec := readElapsed.Seconds()
			hits := readBytes / int64(r.BlockSize)
			out["read_bytes"] = readBytes
			out["read_elapsed_sec"] = sec
			out["read_throughput_mb_s"] = float64(readBytes) / 1024 / 1024 / sec
			out["read_hits"] = hits
			out["read_misses"] = readErrs
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		return
	}

	fmt.Printf("mode       : %s\n", r.Mode)
	fmt.Printf("block_size : %s\n", humanBytes(int64(r.BlockSize)))
	fmt.Printf("num_jobs   : %d\n", r.NumJobs)

	if writeElapsed > 0 {
		sec := writeElapsed.Seconds()
		mbps := float64(writeBytes) / 1024 / 1024 / sec
		iops := float64(writeBytes) / float64(r.BlockSize) / sec
		line := fmt.Sprintf("write      : %s in %.2f s → %.2f MB/s  %.1f IOPS", humanBytes(writeBytes), sec, mbps, iops)
		if writeErrs > 0 {
			line += fmt.Sprintf("  errors=%d", writeErrs)
		}
		fmt.Println(line)
	}

	if readElapsed > 0 {
		sec := readElapsed.Seconds()
		hits := readBytes / int64(r.BlockSize)
		total := hits + readErrs
		hitPct := float64(0)
		if total > 0 {
			hitPct = float64(hits) / float64(total) * 100
		}
		mbps := float64(readBytes) / 1024 / 1024 / sec
		iops := float64(readBytes) / float64(r.BlockSize) / sec
		line := fmt.Sprintf("read       : %s in %.2f s → %.2f MB/s  %.1f IOPS  hit=%.0f%%", humanBytes(readBytes), sec, mbps, iops, hitPct)
		if readErrs > 0 {
			line += fmt.Sprintf("  misses=%d", readErrs)
		}
		fmt.Println(line)
	}
}
