# syncnode integration test suite

End-to-end tests run **against a deployed cluster** (you provide the master
+ syncnode endpoints via env vars). Nothing is mocked at the wire level;
the suite exercises the real HTTP/TCP surface.

## Tiers

| Tier        | Time | Scope                                              |
|-------------|------|----------------------------------------------------|
| `smoke`     | <2m  | Health + register + one rule. Run on every deploy. |
| `functional`| ~30m | All single-node features (Phase D-G).              |
| `integration` | ~1h | Multi-node (P1) + security + performance baseline. |
| `chaos`     | 6h+  | Long-running kill / partition / memory check.      |
| `all`       | ~2h  | smoke + functional + integration.                  |

## Quick start

```sh
cp env.example.sh env.sh
$EDITOR env.sh                      # fill in your endpoints + token
source env.sh

./run.sh smoke                      # always run this first
./run.sh functional                 # most-used during dev
./run.sh integration                # before sign-off
./run.sh all                        # full sweep

# single test file:
./functional/20_sync_idempotent.sh

# verbose log
RUN_VERBOSE=1 ./run.sh smoke
```

## Required environment

```
SYNCNODE_HOST       # e.g. 10.0.1.10  (no port)
SYNCNODE_HTTP_PORT  # e.g. 17720
SYNCNODE_TCP_PORT   # e.g. 17710     (used by SEC2 cap test only)
SYNCNODE_TOKEN      # adminToken from sync.json (empty = auth off)

MASTER_HTTP         # e.g. http://10.0.1.1:17010
MASTER_TOKEN        # syncAdminToken from master.json (often same as syncnode)

# For sync/load tests against S3-compatible backend
S3_ENDPOINT         # e.g. http://minio:9000
S3_BUCKET           # e.g. syncnode-test
S3_AK / S3_SK       # credentials env-var NAMES (NOT values; matches s3Defaults.accessKeyEnv)

# Working set
TEST_DATA_DIR       # writable local dir e.g. /tmp/syncnode-test
ALLOWED_ROOT        # the posix.allowedRoots prefix configured on syncnode
                    # TEST_DATA_DIR MUST be under this for local-kind tests.
```

## What gets created on your cluster

Every test prefixes its rule / task IDs with `it-<test-name>-` and cleans up
on exit (via trap). If a test aborts mid-run, residual rules can be wiped
with `./run.sh cleanup`.

## Required external tools

`curl`, `jq`, `bc`, `dd`, `md5sum` (linux: `md5sum`; macOS: `md5 -q`),
`s3cmd` or `aws cli` (for S3 verification). The runner checks for these
at startup and fails fast if anything's missing.

## Adding a test

1. Pick the right tier directory.
2. Number-prefix the filename so order is stable (`NN_short_name.sh`).
3. Source `lib/common.sh` at the top; use the assertion helpers.
4. `trap cleanup_<test_name> EXIT` to remove created resources.
5. Exit 0 on pass, non-zero on fail.

## File map

```
syncnode/test/
├── README.md             this file
├── env.example.sh        copy → env.sh and fill in
├── run.sh                main entry; dispatches to tiers
├── lib/
│   ├── common.sh         set -euo pipefail + helpers + tool check
│   ├── http.sh           curl wrapper, JSON helpers, retry-with-backoff
│   ├── assert.sh         assert_eq / assert_contains / assert_status
│   ├── fixtures.sh       rule JSON builders
│   └── cleanup.sh        wipe-all (called by run.sh cleanup)
├── smoke/
│   ├── 01_health.sh
│   ├── 02_register.sh
│   └── 03_rule_basic.sh
├── functional/
│   ├── 10_rule_crud.sh
│   ├── 11_rule_conflicts.sh
│   ├── 20_sync_idempotent.sh
│   ├── 21_load_temp_rename.sh
│   ├── 22_check_auto_fix.sh
│   ├── 23_retention_g1.sh
│   ├── 30_reload.sh
│   ├── 31_ttl_export.sh
│   └── 32_cancel_queued.sh         # Wave 3 Q1 regression guard
├── integration/
│   ├── 40_dispatch_distribution.sh
│   ├── 41_failover.sh
│   ├── 42_fanout.sh
│   ├── 43_quota_cluster_cap.sh
│   ├── 50_auth.sh
│   ├── 51_tcp_cap.sh
│   ├── 52_body_cap.sh
│   ├── 60_throughput_baseline.sh
│   └── 61_listsync_p99.sh
├── chaos/
│   ├── 70_kill_loop.sh             # 6h kill/restart cycle
│   ├── 71_network_partition.sh     # requires sudo iptables / chaos-mesh
│   └── 72_mem_goroutine_stable.sh  # 24h stability watcher
└── fixtures/
    ├── rule_local_to_s3.template.json
    ├── rule_with_retention.template.json
    └── rule_fanout.template.json
```
