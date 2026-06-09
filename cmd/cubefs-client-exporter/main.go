// cubefs-client-exporter: cubefs 内核 client 的 Prometheus exporter。
//
// 内核 ko 是内核态、不能自起 HTTP server,本 exporter 作为用户态桥接:
// 读 host 的 procfs/sysfs/kmsg + cubefs procfs log,转成 metrics 经 /metrics 暴露,
// Prometheus 直接 scrape。覆盖 S5 监控基建执行文档的阶段 1(节点级)+ 阶段 2(log 解析)。
//
// 数据源(均为只读):
//
//	/host/proc/mounts                     -> 挂载存活
//	/host/sys/module/cubefs/refcnt        -> ko 引用计数(泄漏检测)
//	/host/sys/module/cubefs/srcversion    -> ko 版本(info)
//	/host/proc/uptime                     -> 节点 uptime(panic 重启检测)
//	/host/proc/slabinfo                   -> cubefs slab 占用(内存泄漏)
//	/host/proc/meminfo                    -> Dirty/Writeback(buffered 回写压力)
//	/host/proc/net/tcp{,6}                -> client->master/meta/data 连接数
//	/dev/kmsg                             -> oops/BUG/cubefs error 计数(后台 tail)
//	/proc/fs/cubefs/<vol>/log             -> 每 op latency + 错误码(后台 tail,阶段 2)
package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddr = flag.String("listen", ":9970", "metrics HTTP listen address")
	mountpoint = flag.String("mountpoint", "/mnt/cubefs-kernel", "cubefs kernel mountpoint")
	vol        = flag.String("vol", "cubefs-2rep-vol", "cubefs volume name (for procfs log path)")
	procRoot   = flag.String("proc", "/proc", "host /proc path")
	sysRoot    = flag.String("sys", "/sys", "host /sys path")
	kmsgPath   = flag.String("kmsg", "/dev/kmsg", "kernel message device for oops/BUG counting")
	nodeName   = flag.String("node", hostnameOrEnv(), "node label value")
	// master/meta/data 监听端口,用于按 peer 归类 client 连接
	masterPort = flag.String("master-port", "17010", "master listen port")
	metaPort   = flag.String("meta-port", "17210", "metanode listen port")
	dataPort   = flag.String("data-port", "17310", "datanode listen port")
)

// 累计计数器(后台 goroutine 维护,Collect 时读取)
var (
	oopsTotal   uint64 // 内核 oops
	bugTotal    uint64 // 内核 BUG()
	cfsErrTotal uint64 // dmesg 里的 cfs/cubefs error
)

func hostnameOrEnv() string {
	if v := os.Getenv("NODE_NAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}

// ---- Collector:scrape 时读 procfs/sysfs 的瞬时值 ----

type collector struct {
	up         *prometheus.Desc
	refcnt     *prometheus.Desc
	koInfo     *prometheus.Desc
	uptime     *prometheus.Desc
	slabBytes  *prometheus.Desc
	dirty      *prometheus.Desc
	writeback  *prometheus.Desc
	sockCount  *prometheus.Desc
	oops       *prometheus.Desc
	bug        *prometheus.Desc
	cfsErr     *prometheus.Desc
	// 阶段 3:读内核 /proc/fs/cubefs/<vol>/stats(数值化,脱离 DEBUG log)
	opTotal   *prometheus.Desc
	opLatency *prometheus.Desc
	opErrors  *prometheus.Desc
	ioBytes   *prometheus.Desc
	ioOps     *prometheus.Desc
}

func newCollector() *collector {
	l := []string{"node"}
	return &collector{
		up:        prometheus.NewDesc("cubefs_client_up", "1 if cubefs kernel mount is healthy", l, nil),
		refcnt:    prometheus.NewDesc("cubefs_ko_refcnt", "cubefs.ko module reference count", l, nil),
		koInfo:    prometheus.NewDesc("cubefs_ko_info", "cubefs.ko info (srcversion as label)", []string{"node", "srcver"}, nil),
		uptime:    prometheus.NewDesc("cubefs_uptime_seconds", "node uptime in seconds (drop => panic reboot)", l, nil),
		slabBytes: prometheus.NewDesc("cubefs_slab_bytes", "total bytes of cfs/cubefs slab caches", l, nil),
		dirty:     prometheus.NewDesc("cubefs_dirty_bytes", "node dirty page bytes (/proc/meminfo Dirty)", l, nil),
		writeback: prometheus.NewDesc("cubefs_writeback_bytes", "node writeback bytes (/proc/meminfo Writeback)", l, nil),
		sockCount: prometheus.NewDesc("cubefs_sock_count", "client TCP connections by peer role", []string{"node", "peer"}, nil),
		oops:      prometheus.NewDesc("cubefs_oops_total", "kernel oops count since exporter start", l, nil),
		bug:       prometheus.NewDesc("cubefs_bug_total", "kernel BUG() count since exporter start", l, nil),
		cfsErr:    prometheus.NewDesc("cubefs_kmsg_errors_total", "cfs/cubefs error lines in kmsg since start", l, nil),
		opTotal:   prometheus.NewDesc("cubefs_op_total", "cubefs client VFS op count by op (from kernel stats)", []string{"node", "op"}, nil),
		opLatency: prometheus.NewDesc("cubefs_op_latency_seconds", "cubefs client VFS op latency histogram by op (from kernel stats)", []string{"node", "op"}, nil),
		opErrors:  prometheus.NewDesc("cubefs_op_errors_total", "cubefs client VFS op error count by op (from kernel stats)", []string{"node", "op"}, nil),
		ioBytes:   prometheus.NewDesc("cubefs_io_bytes_total", "cubefs client data IO bytes by rw (from kernel stats)", []string{"node", "rw"}, nil),
		ioOps:     prometheus.NewDesc("cubefs_io_ops_total", "cubefs client data IO ops by rw (from kernel stats)", []string{"node", "rw"}, nil),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.refcnt
	ch <- c.koInfo
	ch <- c.uptime
	ch <- c.slabBytes
	ch <- c.dirty
	ch <- c.writeback
	ch <- c.sockCount
	ch <- c.oops
	ch <- c.bug
	ch <- c.cfsErr
	ch <- c.opTotal
	ch <- c.opLatency
	ch <- c.opErrors
	ch <- c.ioBytes
	ch <- c.ioOps
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	n := *nodeName
	g := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	cnt := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}

	g(c.up, boolf(mountHealthy()), n)
	if v, ok := readUint(filepath.Join(*sysRoot, "module/cubefs/refcnt")); ok {
		g(c.refcnt, float64(v), n)
	}
	if sv := readTrim(filepath.Join(*sysRoot, "module/cubefs/srcversion")); sv != "" {
		g(c.koInfo, 1, n, sv)
	}
	if up := nodeUptime(); up > 0 {
		g(c.uptime, up, n)
	}
	g(c.slabBytes, float64(cubefsSlabBytes()), n)
	d, w := dirtyWriteback()
	g(c.dirty, float64(d), n)
	g(c.writeback, float64(w), n)
	m, mt, dt := sockCounts()
	g(c.sockCount, float64(m), n, "master")
	g(c.sockCount, float64(mt), n, "meta")
	g(c.sockCount, float64(dt), n, "data")
	cnt(c.oops, float64(atomic.LoadUint64(&oopsTotal)), n)
	cnt(c.bug, float64(atomic.LoadUint64(&bugTotal)), n)
	cnt(c.cfsErr, float64(atomic.LoadUint64(&cfsErrTotal)), n)
	c.collectStats(ch, n)
}

// ---- 数据源读取 ----

func boolf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func readTrim(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readUint(p string) (uint64, bool) {
	s := readTrim(p)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// mountHealthy: /proc/mounts 中 mountpoint 存在且 fstype=cubefs
func mountHealthy() bool {
	f, err := os.Open(filepath.Join(*procRoot, "mounts"))
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 3 && fields[1] == *mountpoint && fields[2] == "cubefs" {
			return true
		}
	}
	return false
}

func nodeUptime() float64 {
	s := readTrim(filepath.Join(*procRoot, "uptime"))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(strings.Fields(s)[0], 64)
	if err != nil {
		return 0
	}
	return v
}

// cubefsSlabBytes: /proc/slabinfo 中 cfs_/cubefs 前缀的 slab 合计字节
// 格式: name <active_objs> <num_objs> <objsize> <objperslab> ...
func cubefsSlabBytes() uint64 {
	f, err := os.Open(filepath.Join(*procRoot, "slabinfo"))
	if err != nil {
		return 0
	}
	defer f.Close()
	var total uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cfs_") && !strings.HasPrefix(line, "cubefs") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		num, _ := strconv.ParseUint(fields[2], 10, 64)
		size, _ := strconv.ParseUint(fields[3], 10, 64)
		total += num * size
	}
	return total
}

func dirtyWriteback() (uint64, uint64) {
	f, err := os.Open(filepath.Join(*procRoot, "meminfo"))
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	var dirty, wb uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		// 值单位 kB
		switch fields[0] {
		case "Dirty:":
			v, _ := strconv.ParseUint(fields[1], 10, 64)
			dirty = v * 1024
		case "Writeback:":
			v, _ := strconv.ParseUint(fields[1], 10, 64)
			wb = v * 1024
		}
	}
	return dirty, wb
}

// sockCounts: 解析 /proc/net/tcp{,6},按 remote 端口归类 master/meta/data 连接数
func sockCounts() (master, meta, data int) {
	want := map[string]*int{
		hexPort(*masterPort): &master,
		hexPort(*metaPort):   &meta,
		hexPort(*dataPort):   &data,
	}
	for _, name := range []string{"net/tcp", "net/tcp6"} {
		f, err := os.Open(filepath.Join(*procRoot, name))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		first := true
		for sc.Scan() {
			if first { // 跳过表头
				first = false
				continue
			}
			fields := strings.Fields(sc.Text())
			if len(fields) < 4 {
				continue
			}
			// fields[2] = rem_address = IP:PORT(hex);fields[3]=st(01=ESTABLISHED)
			if fields[3] != "01" {
				continue
			}
			parts := strings.Split(fields[2], ":")
			if len(parts) != 2 {
				continue
			}
			if p, ok := want[strings.ToUpper(parts[1])]; ok {
				*p++
			}
		}
		f.Close()
	}
	return
}

// hexPort: 十进制端口转 /proc/net/tcp 用的大写 4 位 16 进制
func hexPort(dec string) string {
	v, err := strconv.Atoi(dec)
	if err != nil {
		return ""
	}
	return strings.ToUpper(strconv.FormatInt(int64(v), 16))
}

// ---- 后台:tail /dev/kmsg 累计 oops/BUG/cfs error ----

func watchKmsg() {
	f, err := os.Open(*kmsgPath)
	if err != nil {
		log.Printf("open kmsg %s failed: %v (oops/bug counting disabled)", *kmsgPath, err)
		return
	}
	defer f.Close()
	// seek 到 buffer 末尾,只统计 exporter 启动后的新消息(避免重启时把历史
	// oops/BUG 重新计入导致 counter 跳变误告警)。/dev/kmsg 支持 SEEK_END。
	_, _ = f.Seek(0, io.SeekEnd)
	// 每条消息一行,格式 "prio,seq,ts,flag;message"
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		msg := line
		if i := strings.IndexByte(line, ';'); i >= 0 {
			msg = line[i+1:]
		}
		lower := strings.ToLower(msg)
		switch {
		case strings.Contains(msg, "kernel BUG") || strings.Contains(lower, "bug at"):
			atomic.AddUint64(&bugTotal, 1)
		case strings.Contains(lower, "oops") || strings.Contains(lower, "call trace"):
			atomic.AddUint64(&oopsTotal, 1)
		case strings.Contains(lower, "cfs:") || strings.Contains(lower, "cubefs"):
			if strings.Contains(lower, "error") || strings.Contains(lower, "fail") {
				atomic.AddUint64(&cfsErrTotal, 1)
			}
		}
	}
}

// ---- 阶段 3:读内核 /proc/fs/cubefs/<vol>/stats(数值化快照,脱离 DEBUG log) ----

// 默认 latency 桶(秒),与内核 cfs_lat_bound_us 对应的有限上界;若 stats 输出
// "buckets" 行则以其为准(内核/exporter 解耦,改桶边界只改内核)。
var defaultLatBounds = []float64{1e-3, 5e-3, 1e-2, 5e-2, 0.1, 0.5, 1.0}

// stats 快照格式(纯数值,空格分列):
//
//	version 1
//	buckets 1000 5000 10000 50000 100000 500000 1000000 +Inf
//	op <name> <count> <sum_us> <errs> <b0> ... <b7>
//	io read  <ops> <bytes>
//	io write <ops> <bytes>
//
// 每次 Collect(scrape)同步读一次(幂等快照,非流式 tail);未挂载读失败则跳过 op
// metrics,不影响节点级。histogram 用桶累积和作 count(==Σ桶含+Inf),规避内核
// count/bucket 两 atomic 在 scrape 瞬间的 off-by-one(否则 client_golang 校验 panic)。
func (c *collector) collectStats(ch chan<- prometheus.Metric, n string) {
	f, err := os.Open(filepath.Join(*procRoot, "fs/cubefs", *vol, "stats"))
	if err != nil {
		return // 未挂载/无 stats 节点(老 ko):跳过 op metrics
	}
	defer f.Close()

	bounds := defaultLatBounds
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "buckets":
			var b []float64
			for _, s := range fields[1:] {
				if s == "+Inf" {
					break
				}
				if us, e := strconv.ParseFloat(s, 64); e == nil {
					b = append(b, us/1e6)
				}
			}
			if len(b) > 0 {
				bounds = b
			}
		case "op":
			// op <name> count sum_us errs b0..bN
			if len(fields) < 5 {
				continue
			}
			name := fields[1]
			count, _ := strconv.ParseUint(fields[2], 10, 64)
			sumUs, _ := strconv.ParseFloat(fields[3], 64)
			errs, _ := strconv.ParseFloat(fields[4], 64)
			ch <- prometheus.MustNewConstMetric(c.opTotal, prometheus.CounterValue, float64(count), n, name)
			ch <- prometheus.MustNewConstMetric(c.opErrors, prometheus.CounterValue, errs, n, name)
			// 互斥分桶 → 累积 le 桶;count 取桶累积和(含 +Inf)
			buckets := fields[5:]
			le := make(map[float64]uint64, len(bounds))
			var cum uint64
			for i := 0; i < len(bounds) && i < len(buckets); i++ {
				bv, _ := strconv.ParseUint(buckets[i], 10, 64)
				cum += bv
				le[bounds[i]] = cum
			}
			if len(buckets) > len(bounds) { // +Inf 桶
				bv, _ := strconv.ParseUint(buckets[len(bounds)], 10, 64)
				cum += bv
			}
			ch <- prometheus.MustNewConstHistogram(c.opLatency, cum, sumUs/1e6, le, n, name)
		case "io":
			// io read|write ops bytes
			if len(fields) < 4 {
				continue
			}
			rw := fields[1]
			ops, _ := strconv.ParseFloat(fields[2], 64)
			bytes, _ := strconv.ParseFloat(fields[3], 64)
			ch <- prometheus.MustNewConstMetric(c.ioOps, prometheus.CounterValue, ops, n, rw)
			ch <- prometheus.MustNewConstMetric(c.ioBytes, prometheus.CounterValue, bytes, n, rw)
		}
	}
}

func main() {
	flag.Parse()
	reg := prometheus.NewRegistry()
	reg.MustRegister(newCollector())

	go watchKmsg()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	log.Printf("cubefs-client-exporter listening on %s (node=%s mount=%s vol=%s)",
		*listenAddr, *nodeName, *mountpoint, *vol)
	if err := http.ListenAndServe(*listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}
