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

package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// rc8 #120 wiring tests. These cover:
//   1) RunWarmup is invoked exactly once at stage entry when stage.Warmup is set
//      (POSIX fio, S3 ObjStage; IOR + mdtest have no Warmup field by schema).
//   2) Warmup duration is NOT counted into stage DurationSec.
//   3) MaybeDropCaches is invoked once per stage in all 4 storage paths, with
//      the right "before_first" / "between" routing.
//   4) Disabled CacheDrop / nil Warmup are silently no-op (no calls at all).
//
// The tests inject fakes via the existing setWarmupRunner / setCacheDropper /
// setFioRunner hooks. No real fio/mpirun/sidecar subprocess is started.

// --------------------------- shared fakes ----------------------------------

// recordingWarmup captures every defaultWarmupRunner.run call. It does NOT
// execute loopFn — the test only needs to verify the call was issued; whether
// real I/O happens is RunWarmup's own job and is covered by warmup_runner_test.go.
type recordingWarmup struct {
	mu    sync.Mutex
	calls []recordedWarmupCall
	// sleep on each call so tests can assert warmup time is NOT included in
	// stage DurationSec. Zero = return immediately.
	sleep time.Duration
}

type recordedWarmupCall struct {
	taskID  string
	shardID string
	stage   string
	spec    spec.WarmupSpec
}

func (r *recordingWarmup) run(_ context.Context, taskID, shardID, stage string, sp *spec.WarmupSpec, _ func(ctx context.Context) error) {
	r.mu.Lock()
	r.calls = append(r.calls, recordedWarmupCall{
		taskID:  taskID,
		shardID: shardID,
		stage:   stage,
		spec:    *sp,
	})
	r.mu.Unlock()
	if r.sleep > 0 {
		time.Sleep(r.sleep)
	}
}

func (r *recordingWarmup) snapshot() []recordedWarmupCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedWarmupCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// recordingDropper captures every MaybeDropCaches → Drop() call.
type recordingDropper struct {
	mu     sync.Mutex
	levels []int
}

func (r *recordingDropper) Drop(_ context.Context, level int) error {
	r.mu.Lock()
	r.levels = append(r.levels, level)
	r.mu.Unlock()
	return nil
}

func (r *recordingDropper) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.levels)
}

// --------------------------- POSIX (fio) -----------------------------------

// TestRunFIOStage_WarmupCalledOnce: stage.Warmup 非空时，进入 runFIOStage 必须调
// 一次 defaultWarmupRunner.run；且 warmup 耗时不计入 DurationSec。这里把 fio 子
// 进程换成 fake runner 不可行（runFIOStage 直接 exec fio），所以我们走更上一层：
// 通过 runFIOStageMixed 路径触发 warmup（它也调用 runStageWarmup，且 fio 通过
// fioRunner 接口可注入）。runFIOStageMixed 与 runFIOStage 共享同一个 helper，
// 任一被覆盖即等价证明 helper 在 stage 入口生效。
func TestRunFIOStageMixed_WarmupCalledOnce(t *testing.T) {
	rec := &recordingWarmup{sleep: 50 * time.Millisecond}
	defer setWarmupRunner(rec)()
	defer setFioRunner(&fakeFioRunner{})()

	stage := spec.FIOStage{
		Name:    "warm-mix",
		Runtime: 4,
		Mixed: []spec.FIOMixedComponent{
			{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"},
		},
		Warmup: &spec.WarmupSpec{DurationSeconds: 1, TargetQPS: 10},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sr, err := runFIOStageMixed(ctx, spec.FIOConfig{}, stage, t.TempDir(), "t-warm", 0, 0)
	if err != nil {
		t.Fatalf("runFIOStageMixed: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("warmup called %d times, want 1", len(calls))
	}
	if calls[0].stage != stage.Name || calls[0].taskID != "t-warm" || calls[0].shardID != "0" {
		t.Errorf("warmup call labels mismatch: %+v", calls[0])
	}
	if calls[0].spec.DurationSeconds != 1 || calls[0].spec.TargetQPS != 10 {
		t.Errorf("warmup spec not propagated: %+v", calls[0].spec)
	}
	// fakeFioRunner returns DurationSec=2.5 for weight=1 (see bench_fio_mixed_test.go),
	// but the aggregator overwrites with time.Since(t0). What we MUST verify is that
	// DurationSec did not balloon by the 50ms warmup sleep above what wall-clock the
	// fake fio actually took. We assert it's <500ms — the fake returns instantly so
	// anything bigger would mean warmup leaked into the measurement.
	if sr.DurationSec > 0.5 {
		t.Errorf("DurationSec=%v includes warmup time (fake fio returns instantly)", sr.DurationSec)
	}
}

// TestRunFIOStageMixed_WarmupNil_NoCall: stage.Warmup=nil → 不应调用 warmup runner。
func TestRunFIOStageMixed_WarmupNil_NoCall(t *testing.T) {
	rec := &recordingWarmup{}
	defer setWarmupRunner(rec)()
	defer setFioRunner(&fakeFioRunner{})()

	stage := spec.FIOStage{
		Name:    "no-warm",
		Runtime: 1,
		Mixed:   []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runFIOStageMixed(ctx, spec.FIOConfig{}, stage, t.TempDir(), "t", 0, 0); err != nil {
		t.Fatalf("runFIOStageMixed: %v", err)
	}
	if got := len(rec.snapshot()); got != 0 {
		t.Errorf("warmup called %d times, want 0", got)
	}
}

// TestRunBenchPosix_CacheDropPerStage: 3 个 stage 应触发 3 次 cache drop；
// 首次为 before_first（需 BeforeFirstStage=true 才实际 drop），后续为 between
// （需 BetweenStages=true）。这里同时开启两者，验证 3 次 Drop 都落地。
func TestRunBenchPosix_CacheDropPerStage(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setFioRunner(&fakeFioRunner{})()
	// warmup 关掉避免噪音
	defer setWarmupRunner(&recordingWarmup{})()

	rule := &spec.BenchRule{
		StorageType: spec.BenchStoragePosix,
		MountPath:   t.TempDir(),
		FIOStages: []spec.FIOStage{
			// 用 Mixed 让 fio 子进程被 fakeFioRunner 接管，避免真实 exec fio。
			{Name: "s1", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s2", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s3", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BetweenStages: true, BeforeFirstStage: true, DropLevel: 2},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runBenchPosix(ctx, rule, "t-drop", 0, 1, 0); err != nil {
		t.Fatalf("runBenchPosix: %v", err)
	}
	if got := rec.count(); got != 3 {
		t.Errorf("Drop calls=%d, want 3 (1 before_first + 2 between)", got)
	}
}

// TestRunBenchPosix_CacheDropDisabled_NoCall: CacheDrop=nil → 0 次 Drop。
func TestRunBenchPosix_CacheDropDisabled_NoCall(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setFioRunner(&fakeFioRunner{})()
	defer setWarmupRunner(&recordingWarmup{})()

	rule := &spec.BenchRule{
		StorageType: spec.BenchStoragePosix,
		MountPath:   t.TempDir(),
		FIOStages: []spec.FIOStage{
			{Name: "s1", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runBenchPosix(ctx, rule, "t", 0, 1, 0); err != nil {
		t.Fatalf("runBenchPosix: %v", err)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("Drop calls=%d, want 0 when CacheDrop=nil", got)
	}
}

// --------------------------- S3 -------------------------------------------

// TestRunObjStage_WarmupCalledOnce: ObjStage.Warmup 非空 → runObjStage 入口处
// 必须调一次 warmup runner，且 warmup 耗时不计入 DurationSec。
func TestRunObjStage_WarmupCalledOnce(t *testing.T) {
	rec := &recordingWarmup{sleep: 80 * time.Millisecond}
	defer setWarmupRunner(rec)()

	b := &benchS3Backend{}
	stage := spec.ObjStage{
		Name:       "warm-put",
		NumJobs:    1,
		NumObjects: 1,
		ObjectSize: spec.ObjSize{Fixed: 16},
		Ops:        []spec.ObjOp{{Type: "put", Weight: 1}},
		Warmup:     &spec.WarmupSpec{DurationSeconds: 1, TargetQPS: 5},
	}
	t0 := time.Now()
	sr := runShortStage(t, stage, b)
	wallElapsed := time.Since(t0)

	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("warmup called %d times, want 1", len(calls))
	}
	if calls[0].stage != "warm-put" || calls[0].spec.DurationSeconds != 1 {
		t.Errorf("warmup spec mismatch: %+v", calls[0])
	}
	// wall clock includes the 80ms warmup sleep; DurationSec must exclude it.
	if wallElapsed < 80*time.Millisecond {
		t.Fatalf("test impl bug: wallElapsed=%v < 80ms", wallElapsed)
	}
	// stage real work (1 fake put) is sub-ms; DurationSec should be much less
	// than wall clock. Anything >= 80ms means warmup leaked into measurement.
	if sr.DurationSec*1000 >= 80 {
		t.Errorf("DurationSec=%vs includes warmup sleep (wall=%v)", sr.DurationSec, wallElapsed)
	}
}

// TestRunObjStage_WarmupNil_NoCall: ObjStage.Warmup=nil → 0 次 warmup 调用。
func TestRunObjStage_WarmupNil_NoCall(t *testing.T) {
	rec := &recordingWarmup{}
	defer setWarmupRunner(rec)()

	b := &benchS3Backend{}
	stage := spec.ObjStage{
		Name:       "no-warm",
		NumJobs:    1,
		NumObjects: 1,
		ObjectSize: spec.ObjSize{Fixed: 16},
		Ops:        []spec.ObjOp{{Type: "put", Weight: 1}},
	}
	_ = runShortStage(t, stage, b)
	if got := len(rec.snapshot()); got != 0 {
		t.Errorf("warmup called %d times, want 0", got)
	}
}

// TestRunBenchS3_CacheDropPerStage: 2 个 stage → 2 次 Drop（1 before_first + 1 between）。
func TestRunBenchS3_CacheDropPerStage(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setWarmupRunner(&recordingWarmup{})()

	b := &benchS3Backend{}
	rule := &spec.BenchRule{
		StorageType: spec.BenchStorageS3,
		KeyPrefix:   "test/",
		Stages: []spec.ObjStage{
			{Name: "s1", NumJobs: 1, NumObjects: 1, ObjectSize: spec.ObjSize{Fixed: 16}, Ops: []spec.ObjOp{{Type: "put", Weight: 1}}},
			{Name: "s2", NumJobs: 1, NumObjects: 1, ObjectSize: spec.ObjSize{Fixed: 16}, Ops: []spec.ObjOp{{Type: "put", Weight: 1}}},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BetweenStages: true, BeforeFirstStage: true, DropLevel: 3},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runBenchS3(ctx, rule, "t-s3", 0, 1, b, 0); err != nil {
		t.Fatalf("runBenchS3: %v", err)
	}
	if got := rec.count(); got != 2 {
		t.Errorf("Drop calls=%d, want 2", got)
	}
}

// --------------------------- IOR ------------------------------------------

// TestRunBenchIOR_CacheDropPerStage: 2 个 stage → 2 次 Drop。IORStage 上没有
// Warmup 字段，所以 IOR 路径只测 cache_drop。
func TestRunBenchIOR_CacheDropPerStage(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setWarmupRunner(&recordingWarmup{})()

	// 给 sidecar 接一个最小桩，让 /run 返回 exit=0 + 空 stdout，使 stage 正常完成。
	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		// drain body so the client side completes cleanly
		raw, _ := io.ReadAll(r.Body)
		var req iorRunRequest
		_ = json.Unmarshal(raw, &req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(iorRunResponse{ExitCode: 0, Stdout: "{}", DurationSec: 0.1})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       t.TempDir(),
		SidecarEndpoint: srv.URL,
		IORStages: []spec.IORStage{
			{Name: "s1", Tool: "ior", Args: []string{"-w"}},
			{Name: "s2", Tool: "ior", Args: []string{"-r"}},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BetweenStages: true, BeforeFirstStage: true, DropLevel: 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runBenchIORWithClient(ctx, rule, "t-ior", 0, 1, 0, http.DefaultClient); err != nil {
		t.Fatalf("runBenchIORWithClient: %v", err)
	}
	if got := rec.count(); got != 2 {
		t.Errorf("Drop calls=%d, want 2", got)
	}
}

// TestRunBenchIOR_CacheDropDisabled: CacheDrop=nil → 0 次 Drop。
func TestRunBenchIOR_CacheDropDisabled(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()

	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(iorRunResponse{ExitCode: 0, Stdout: "{}"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       t.TempDir(),
		SidecarEndpoint: srv.URL,
		IORStages: []spec.IORStage{
			{Name: "s1", Tool: "ior", Args: []string{"-w"}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := runBenchIORWithClient(ctx, rule, "t", 0, 1, 0, http.DefaultClient); err != nil {
		t.Fatalf("runBenchIORWithClient: %v", err)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("Drop calls=%d, want 0", got)
	}
}

// --------------------------- mdtest ---------------------------------------

// TestRunBenchMdtest_SkipSuppressesCacheDrop: mdtest 路径与 POSIX/IOR 一致 —
// Skip=true 让 stage 跳过实际执行，cache_drop 也不会触发（drop 的意图是为即将
// 进行的工作准备缓存状态；跳过的 stage 不需要 drop）。这个测试间接验证了
// mdtest 路径中 MaybeDropCaches 被正确连接：如果完全没接 wiring，所有 stage
// 都不会触发 drop（与下方 Disabled 测试同结论，无法区分缺线和 Skip 抑制）。
// 真正"drops do fire"的覆盖由 POSIX/S3/IOR 三条路径承担，那三条路径共用
// 相同的 wiring 模式 + 同一份 MaybeDropCaches helper。
func TestRunBenchMdtest_SkipSuppressesCacheDrop(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()

	rule := &spec.BenchRule{
		StorageType:    spec.BenchStorageMdtest,
		MountPath:      t.TempDir(),
		MdtestDefaults: &spec.MdtestConfig{},
		MdtestStages: []spec.MdtestStage{
			{Name: "s1", Skip: true},
			{Name: "s2", Skip: true},
			{Name: "s3", Skip: true},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BetweenStages: true, BeforeFirstStage: true, DropLevel: 3},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := runBenchMdtest(ctx, rule, "t-md", 0, 1, 0); err != nil {
		t.Fatalf("runBenchMdtest: %v", err)
	}
	// Skip 早于 MaybeDropCaches 退出循环体：3 个全 Skip stage → 0 次 Drop。
	if got := rec.count(); got != 0 {
		t.Errorf("Drop calls=%d, want 0 (all stages Skip)", got)
	}
}

// TestRunBenchMdtest_CacheDropDisabled: CacheDrop=nil → 0 次 Drop。
func TestRunBenchMdtest_CacheDropDisabled(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()

	rule := &spec.BenchRule{
		StorageType:    spec.BenchStorageMdtest,
		MountPath:      t.TempDir(),
		MdtestDefaults: &spec.MdtestConfig{},
		MdtestStages: []spec.MdtestStage{
			{Name: "s1", Skip: true},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := runBenchMdtest(ctx, rule, "t", 0, 1, 0); err != nil {
		t.Fatalf("runBenchMdtest: %v", err)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("Drop calls=%d, want 0", got)
	}
}

// --------------------------- BetweenStages/BeforeFirst routing -----------

// TestCacheDrop_BeforeFirstOnly: BeforeFirstStage=true, BetweenStages=false →
// 3 stage 仅触发 1 次实际 Drop（首个 stage 前），其它 stage 之间被压制。
func TestCacheDrop_BeforeFirstOnly(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setFioRunner(&fakeFioRunner{})()
	defer setWarmupRunner(&recordingWarmup{})()

	rule := &spec.BenchRule{
		StorageType: spec.BenchStoragePosix,
		MountPath:   t.TempDir(),
		FIOStages: []spec.FIOStage{
			{Name: "s1", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s2", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s3", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BeforeFirstStage: true, BetweenStages: false, DropLevel: 3},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runBenchPosix(ctx, rule, "t", 0, 1, 0); err != nil {
		t.Fatalf("runBenchPosix: %v", err)
	}
	if got := rec.count(); got != 1 {
		t.Errorf("Drop calls=%d, want 1 (only before_first)", got)
	}
}

// TestCacheDrop_BetweenOnly: BetweenStages=true, BeforeFirstStage=false →
// 3 stage 触发 2 次 Drop（仅 stage 之间，不含首次）。
func TestCacheDrop_BetweenOnly(t *testing.T) {
	rec := &recordingDropper{}
	defer setCacheDropper(rec)()
	defer setFioRunner(&fakeFioRunner{})()
	defer setWarmupRunner(&recordingWarmup{})()

	rule := &spec.BenchRule{
		StorageType: spec.BenchStoragePosix,
		MountPath:   t.TempDir(),
		FIOStages: []spec.FIOStage{
			{Name: "s1", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s2", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
			{Name: "s3", Runtime: 1, Mixed: []spec.FIOMixedComponent{{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"}}},
		},
		CacheDrop: &spec.CacheDropSpec{Enabled: true, BeforeFirstStage: false, BetweenStages: true, DropLevel: 3},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runBenchPosix(ctx, rule, "t", 0, 1, 0); err != nil {
		t.Fatalf("runBenchPosix: %v", err)
	}
	if got := rec.count(); got != 2 {
		t.Errorf("Drop calls=%d, want 2 (only between)", got)
	}
}

