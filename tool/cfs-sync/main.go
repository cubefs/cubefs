package main

import (
	"fmt"
	"os"
)

const usage = `cfs-sync — CubeFS data sync and I/O tool (no FUSE, direct SDK)

Usage:
  cfs-sync sync  <src> <dst> [flags]   Sync src to dst (delete extra files at dst)
  cfs-sync check <src> <dst> [flags]   Dry-run check without transferring
  cfs-sync bench [flags]               Run a read/write benchmark against CubeFS or FlashNode
  cfs-sync read  [flags]               Read a CubeFS file to stdout or a local file
  cfs-sync write [flags]               Write stdin or a local file to a CubeFS path

URI formats (sync/check):
  cfs://vol/path/to/dir        CubeFS volume (requires --master)
  s3://bucket/prefix/          S3-compatible storage
  /absolute/path/              Local filesystem
  ./relative/path/             Local filesystem (relative)

Global config:
  ~/.cfs-cli.json              Optional config file (masterAddr, timeout)

Examples:
  cfs-sync sync s3://my-bucket/data/ cfs://my-vol/data/ --master 10.0.0.1:17010
  cfs-sync sync cfs://vol/ckpt/ /backup/ --include "*.pt" --max-age 720h
  cfs-sync bench --vol my-vol --mode rw --bs 4M --numjobs 8
  cfs-sync bench --flash --mode rw --bs 4M --numjobs 8
  cfs-sync read  --vol my-vol --path /data/model.pt --out ./model.pt
  cfs-sync write --vol my-vol --path /data/model.pt --in ./model.pt

Run 'cfs-sync <subcommand> -h' for all flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	// Load ~/.cfs-cli.json once at startup so we can initialise the
	// RDMA pool (process-global) before any subcommand builds its
	// ExtentClient. When the file is missing or rdmaEnable is false
	// this is a silent no-op; existing TCP-only setups need no config
	// change.
	if cfg, err := loadCLIConfig(); err == nil {
		initRDMAFromConfig(cfg)
	}

	sub := os.Args[1]
	args := os.Args[2:]

	switch sub {
	case "sync":
		runSync(args, true)
	case "check":
		runCheck(args)
	case "bench":
		runBench(args)
	case "read":
		runRead(args)
	case "write":
		runWrite(args)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", sub, usage)
		os.Exit(2)
	}
}
