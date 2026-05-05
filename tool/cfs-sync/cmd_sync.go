package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/tool/cfs-sync/storage"
)

func runSync(args []string, deleteByDefault bool) {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)

	// Connection
	master := fs.String("master", "", "CubeFS master addresses (comma-separated)")
	endpoint := fs.String("endpoint", "", "S3 endpoint URL (MinIO/OSS)")
	accessKey := fs.String("access-key", "", "S3 access key")
	secretKey := fs.String("secret-key", "", "S3 secret key")
	region := fs.String("region", "us-east-1", "S3 region")
	noSSL := fs.Bool("no-ssl", false, "Disable TLS for S3")

	// Concurrency
	transfers := fs.Int("transfers", 10, "Number of parallel transfer workers")
	checkers := fs.Int("checkers", 20, "Number of parallel checker workers")
	listWorkers := fs.Int("list-workers", 20, "Number of parallel list workers")

	// Comparison
	sizeOnly := fs.Bool("size-only", false, "Compare by size only (skip mtime)")
	checksum := fs.Bool("checksum", false, "Compare by MD5 checksum")
	ignoreExisting := fs.Bool("ignore-existing", false, "Skip files that already exist at destination")

	// Delete
	deleteFlag := fs.Bool("delete", deleteByDefault, "Delete destination files not in source")
	backupDir := fs.String("backup-dir", "", "Move overwritten/deleted files here instead of deleting")
	suffix := fs.String("suffix", "", "Suffix to append to backup files")

	// Transfer control
	partSize := fs.Int64("part-size", 64*1024*1024, "Multipart upload part size")
	multiThreadCutoff := fs.Int64("multi-thread-cutoff", 256*1024*1024, "File size threshold for multi-thread transfer")

	// Filters
	include := fs.String("include", "", "Include files matching glob (comma-separated)")
	exclude := fs.String("exclude", "", "Exclude files matching glob (comma-separated)")
	filesFrom := fs.String("files-from", "", "Read list of files to transfer from this file")
	minSizeStr := fs.String("min-size", "", "Skip files smaller than this (e.g. 1M)")
	maxSizeStr := fs.String("max-size", "", "Skip files larger than this (e.g. 10G)")
	minAgeStr := fs.String("min-age", "", "Skip files newer than this (e.g. 1h)")
	maxAgeStr := fs.String("max-age", "", "Skip files older than this (e.g. 7d)")

	// Limits
	maxTransferStr := fs.String("max-transfer", "", "Stop after transferring this total amount (e.g. 100G)")
	maxDurationStr := fs.String("max-duration", "", "Stop after this duration (e.g. 2h)")

	// Retry
	retries := fs.Int("retries", 3, "Number of retries per file")
	retrySleep := fs.Duration("retries-sleep", time.Second, "Initial retry sleep (exponential backoff)")
	ignoreErrors := fs.Bool("ignore-errors", false, "Continue on per-file errors")

	// Output
	dryRun := fs.Bool("dry-run", false, "Print actions without executing")
	progress := fs.Bool("progress", false, "Show real-time progress")
	statsInterval := fs.Duration("stats", time.Minute, "Interval for periodic stats output")
	logDir := fs.String("log-dir", "/tmp/cfs-sync-logs", "SDK log directory")
	logLevel := fs.String("log-level", "WARN", "Log level")

	_ = fs.Parse(args)

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "error: src and dst are required\n\nUsage: cfs-sync sync <src> <dst> [flags]\n")
		fs.Usage()
		os.Exit(2)
	}
	srcURI := fs.Arg(0)
	dstURI := fs.Arg(1)

	opts := SyncOptions{
		SizeOnly:          *sizeOnly,
		Checksum:          *checksum,
		IgnoreExisting:    *ignoreExisting,
		Transfers:         *transfers,
		Checkers:          *checkers,
		ListWorkers:       *listWorkers,
		PartSize:          *partSize,
		MultiThreadCutoff: *multiThreadCutoff,
		Delete:            *deleteFlag,
		BackupDir:         *backupDir,
		Suffix:            *suffix,
		FilesFrom:         *filesFrom,
		Retries:           *retries,
		RetriesSleep:      *retrySleep,
		IgnoreErrors:      *ignoreErrors,
		DryRun:            *dryRun,
		Progress:          *progress,
		Stats:             *statsInterval,
		LogDir:            *logDir,
		LogLevel:          *logLevel,
	}

	// Parse filter strings
	if *include != "" {
		opts.Include = strings.Split(*include, ",")
	}
	if *exclude != "" {
		opts.Exclude = strings.Split(*exclude, ",")
	}
	if *minSizeStr != "" {
		sz, err := parseSize(*minSizeStr)
		dieOnErr(err)
		opts.MinSize = sz
	}
	if *maxSizeStr != "" {
		sz, err := parseSize(*maxSizeStr)
		dieOnErr(err)
		opts.MaxSize = sz
	}
	if *minAgeStr != "" {
		d, err := time.ParseDuration(*minAgeStr)
		dieOnErr(err)
		opts.MinAge = d
	}
	if *maxAgeStr != "" {
		d, err := time.ParseDuration(*maxAgeStr)
		dieOnErr(err)
		opts.MaxAge = d
	}
	if *maxTransferStr != "" {
		sz, err := parseSize(*maxTransferStr)
		dieOnErr(err)
		opts.MaxTransfer = sz
	}
	if *maxDurationStr != "" {
		d, err := time.ParseDuration(*maxDurationStr)
		dieOnErr(err)
		opts.MaxDuration = d
	}

	cfg, err := loadCLIConfig()
	dieOnErr(err)
	masters, _ := resolveMasters(*master, cfg)

	s3cfg := storage.S3Config{
		Endpoint:  *endpoint,
		AccessKey: *accessKey,
		SecretKey: *secretKey,
		Region:    *region,
		NoSSL:     *noSSL,
	}

	src, err := openStorage(srcURI, masters, s3cfg, opts.LogDir, opts.LogLevel)
	dieOnErr(err)
	dst, err := openStorage(dstURI, masters, s3cfg, opts.LogDir, opts.LogLevel)
	dieOnErr(err)

	ctx := context.Background()
	if opts.MaxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.MaxDuration)
		defer cancel()
	}

	syncer := NewSyncer(src, dst, opts)
	failed := syncer.Run(ctx)

	if failed > 0 {
		os.Exit(1)
	}
}

func runCheck(args []string) {
	// check = sync --dry-run --size-only --no-delete
	newArgs := append(args, "--dry-run", "--size-only")
	runSync(newArgs, false)
}

// openStorage parses a URI and returns the corresponding Storage backend.
func openStorage(uri string, masters []string, s3cfg storage.S3Config, logDir, logLevel string) (storage.Storage, error) {
	switch {
	case strings.HasPrefix(uri, "cfs://"):
		// cfs://vol/path/to/dir
		rest := strings.TrimPrefix(uri, "cfs://")
		slash := strings.Index(rest, "/")
		var vol, p string
		if slash < 0 {
			vol, p = rest, "/"
		} else {
			vol, p = rest[:slash], rest[slash:]
		}
		if len(masters) == 0 {
			return nil, fmt.Errorf("--master is required for cfs:// URIs")
		}
		return storage.NewCFS(storage.CFSConfig{
			Masters:  masters,
			Vol:      vol,
			LogDir:   logDir,
			LogLevel: logLevel,
		}, p)

	case strings.HasPrefix(uri, "s3://"):
		// s3://bucket/prefix
		rest := strings.TrimPrefix(uri, "s3://")
		slash := strings.Index(rest, "/")
		var bucket, prefix string
		if slash < 0 {
			bucket, prefix = rest, ""
		} else {
			bucket, prefix = rest[:slash], rest[slash+1:]
		}
		s3cfg.Bucket = bucket
		s, err := storage.NewS3(s3cfg)
		if err != nil {
			return nil, err
		}
		_ = prefix // prefix is used in List calls
		return s, nil

	default:
		// local path
		return storage.NewLocal(uri)
	}
}

func dieOnErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// parseSize parses a human-readable size string like "1M", "10G", "512K".
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	multipliers := map[string]int64{
		"K": 1024, "KB": 1024,
		"M": 1024 * 1024, "MB": 1024 * 1024, "MIB": 1024 * 1024,
		"G": 1024 * 1024 * 1024, "GB": 1024 * 1024 * 1024, "GIB": 1024 * 1024 * 1024,
		"T": 1024 * 1024 * 1024 * 1024,
	}
	upper := strings.ToUpper(s)
	for suffix, mult := range multipliers {
		if strings.HasSuffix(upper, suffix) {
			num := strings.TrimSpace(strings.TrimSuffix(upper, suffix))
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return n * mult, nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n, nil
}
