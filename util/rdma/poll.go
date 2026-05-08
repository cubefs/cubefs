// Adaptive polling state machine for the RDMA transport.
//
// This file is build-tag-free: the state machine itself is pure logic and
// can be unit-tested on any platform. The CGO/RDMA-tagged callers wire the
// "sleep" action to ibv_req_notify_cq + ibv_get_cq_event; tests exercise
// the state transitions without touching the verbs layer.
//
// Three phases (P2 of docs/plan/rdma-optimization-spec.md):
//
//   PhaseBusy  — tight loop, no syscall (lowest latency, highest CPU)
//   PhaseYield — runtime.Gosched per iteration (cooperative wait)
//   PhaseSleep — block on a kernel-level wakeup (idle CPU << 1%)
//
// The phase transitions are governed by:
//
//   BusySpinCount     — max iterations in phase 1 before moving to phase 2
//   YieldCount        — max iterations in phase 2 before moving to phase 3
//   SleepThresholdUs  — time in phase 2 before moving to phase 3
//
// Setting BusySpinCount to a very large value pins the poller in phase 1,
// reproducing pre-P2 pure busy-poll behaviour. Setting BusySpinCount=0 and
// YieldCount=0 produces pure-sleep behaviour. The two parameters are
// independent thresholds in phase 2: whichever fires first transitions to
// phase 3.

package rdma

import (
	"time"
)

// PollConfig holds the three knobs that govern adaptive polling.
type PollConfig struct {
	// BusySpinCount caps how many tight-loop iterations precede yielding.
	// Set to a very large value (e.g. math.MaxInt) for pure busy-poll.
	BusySpinCount int

	// YieldCount caps how many runtime.Gosched iterations precede sleeping.
	// Set to 0 to skip the yield phase entirely.
	YieldCount int

	// SleepThresholdUs is the maximum wall-clock time spent in the yield
	// phase before the poller transitions to sleep. Allows tail-latency
	// callers to bound their wakeup delay independently of YieldCount.
	SleepThresholdUs time.Duration
}

// DefaultPollConfig matches the defaults in P2 of the spec.
var DefaultPollConfig = PollConfig{
	BusySpinCount:    200,
	YieldCount:       1000,
	SleepThresholdUs: 50 * time.Microsecond,
}

// pollPhase captures which phase the AdaptivePoller is currently in.
type pollPhase int

const (
	phaseBusy pollPhase = iota
	phaseYield
	phaseSleep
)

// PollAction is what the caller should do next. The state machine never
// performs the action itself: the caller decides whether to spin, Gosched,
// or block on a comp-channel.
type PollAction int

const (
	// ActionContinue: caller should immediately re-poll (tight loop).
	ActionContinue PollAction = iota
	// ActionYield: caller should call runtime.Gosched then re-poll.
	ActionYield
	// ActionSleep: caller should block until a wakeup event arrives, then
	// re-poll. In RDMA mode this is ibv_req_notify_cq + ibv_get_cq_event.
	ActionSleep
)

// String for log/diagnostics output. Cheap; not on the hot path.
func (a PollAction) String() string {
	switch a {
	case ActionContinue:
		return "continue"
	case ActionYield:
		return "yield"
	case ActionSleep:
		return "sleep"
	default:
		return "unknown"
	}
}

// AdaptivePoller is the per-poll-site state machine. It is NOT goroutine
// safe — each polling goroutine should own its own AdaptivePoller. After
// successfully consuming work, callers must call Reset to return to phase 1.
type AdaptivePoller struct {
	cfg PollConfig

	phase      pollPhase
	busyCount  int
	yieldCount int

	// yieldStart is set when transitioning to phase 2; SleepThresholdUs is
	// measured from this point. Not from the overall poll start, because
	// phase 1 is intentionally "free" (busy spin shouldn't count against
	// the tail-latency budget).
	yieldStart time.Time
}

// NewAdaptivePoller constructs a poller initialised in phase 1. cfg is
// copied; later mutations to the caller's struct do not affect the poller.
func NewAdaptivePoller(cfg PollConfig) *AdaptivePoller {
	return &AdaptivePoller{cfg: cfg}
}

// Reset returns the poller to phase 1 with fresh counters. Call after the
// caller has successfully processed at least one unit of work, so the next
// idle stretch starts from minimum latency again.
func (p *AdaptivePoller) Reset() {
	p.phase = phaseBusy
	p.busyCount = 0
	p.yieldCount = 0
	p.yieldStart = time.Time{}
}

// NextAction advances the state machine by one tick and returns what the
// caller should do.
//
// Contract:
//   - In phase 1, returns ActionContinue at most BusySpinCount times.
//   - When the busy budget is exhausted, transitions to phase 2 and returns
//     ActionYield.
//   - In phase 2, returns ActionYield up to YieldCount times OR up to
//     SleepThresholdUs of elapsed time, whichever comes first.
//   - Once either phase-2 budget is exhausted, returns ActionSleep on every
//     subsequent call until Reset.
func (p *AdaptivePoller) NextAction() PollAction {
	for {
		switch p.phase {
		case phaseBusy:
			if p.busyCount < p.cfg.BusySpinCount {
				p.busyCount++
				return ActionContinue
			}
			p.phase = phaseYield
			p.yieldStart = time.Now()
			// Fall through to evaluate phase 2 immediately so a YieldCount=0
			// configuration cleanly drops to sleep without an extra tick.
		case phaseYield:
			elapsed := time.Since(p.yieldStart)
			if p.yieldCount >= p.cfg.YieldCount || elapsed >= p.cfg.SleepThresholdUs {
				p.phase = phaseSleep
				return ActionSleep
			}
			p.yieldCount++
			return ActionYield
		default: // phaseSleep
			return ActionSleep
		}
	}
}

// InPhase reports the current phase as a string for diagnostics.
func (p *AdaptivePoller) InPhase() string {
	switch p.phase {
	case phaseBusy:
		return "busy"
	case phaseYield:
		return "yield"
	default:
		return "sleep"
	}
}
