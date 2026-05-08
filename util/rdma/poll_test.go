package rdma

import (
	"math"
	"testing"
	"time"
)

func TestAdaptivePoller_StartsInBusyPhase(t *testing.T) {
	p := NewAdaptivePoller(DefaultPollConfig)
	if p.InPhase() != "busy" {
		t.Fatalf("initial phase: got %q want busy", p.InPhase())
	}
	if got := p.NextAction(); got != ActionContinue {
		t.Fatalf("first action: got %v want ActionContinue", got)
	}
}

// TestAdaptivePoller_BusyToYieldTransition verifies that after BusySpinCount
// continues, the poller transitions to yield.
func TestAdaptivePoller_BusyToYieldTransition(t *testing.T) {
	cfg := PollConfig{BusySpinCount: 5, YieldCount: 100, SleepThresholdUs: time.Hour}
	p := NewAdaptivePoller(cfg)

	for i := 0; i < cfg.BusySpinCount; i++ {
		if got := p.NextAction(); got != ActionContinue {
			t.Fatalf("iter %d: got %v want ActionContinue", i, got)
		}
	}
	// Next action should be Yield (transitioned).
	if got := p.NextAction(); got != ActionYield {
		t.Fatalf("after busy budget: got %v want ActionYield", got)
	}
	if p.InPhase() != "yield" {
		t.Fatalf("phase: got %q want yield", p.InPhase())
	}
}

// TestAdaptivePoller_YieldToSleepByCount verifies that after YieldCount yields
// the poller transitions to sleep, regardless of elapsed time.
func TestAdaptivePoller_YieldToSleepByCount(t *testing.T) {
	cfg := PollConfig{BusySpinCount: 0, YieldCount: 3, SleepThresholdUs: time.Hour}
	p := NewAdaptivePoller(cfg)

	// BusySpinCount=0 → first NextAction transitions to yield and returns Yield.
	for i := 0; i < cfg.YieldCount; i++ {
		if got := p.NextAction(); got != ActionYield {
			t.Fatalf("yield iter %d: got %v want ActionYield", i, got)
		}
	}
	// Next call: yield budget exhausted → sleep.
	if got := p.NextAction(); got != ActionSleep {
		t.Fatalf("after yield budget: got %v want ActionSleep", got)
	}
	if p.InPhase() != "sleep" {
		t.Fatalf("phase: got %q want sleep", p.InPhase())
	}
	// Should remain in sleep on subsequent calls.
	if got := p.NextAction(); got != ActionSleep {
		t.Fatalf("subsequent: got %v want ActionSleep", got)
	}
}

// TestAdaptivePoller_YieldToSleepByTime verifies that elapsed time in the
// yield phase forces a sleep transition before YieldCount is reached.
func TestAdaptivePoller_YieldToSleepByTime(t *testing.T) {
	cfg := PollConfig{
		BusySpinCount:    0,
		YieldCount:       math.MaxInt32,
		SleepThresholdUs: 5 * time.Millisecond,
	}
	p := NewAdaptivePoller(cfg)

	// Burn a couple of yields to enter phase 2.
	if got := p.NextAction(); got != ActionYield {
		t.Fatalf("first yield: got %v", got)
	}
	// Sleep beyond the threshold; next action must transition to sleep.
	time.Sleep(cfg.SleepThresholdUs + 5*time.Millisecond)
	if got := p.NextAction(); got != ActionSleep {
		t.Fatalf("after time threshold: got %v want ActionSleep", got)
	}
}

// TestAdaptivePoller_PureBusyPoll verifies the spec requirement that
// BusySpinCount=MaxInt produces pre-P2 pure busy-poll behaviour.
func TestAdaptivePoller_PureBusyPoll(t *testing.T) {
	cfg := PollConfig{
		BusySpinCount:    math.MaxInt,
		YieldCount:       1000,
		SleepThresholdUs: 50 * time.Microsecond,
	}
	p := NewAdaptivePoller(cfg)

	// 10000 calls should all return Continue without ever yielding or
	// sleeping, since the busy budget is effectively unlimited and the
	// SleepThresholdUs only applies in the yield phase.
	for i := 0; i < 10_000; i++ {
		if got := p.NextAction(); got != ActionContinue {
			t.Fatalf("iter %d: got %v want ActionContinue (pure busy mode broken)", i, got)
		}
	}
	if p.InPhase() != "busy" {
		t.Fatalf("phase: got %q want busy", p.InPhase())
	}
}

// TestAdaptivePoller_PureSleep verifies the spec requirement that
// BusySpinCount=0 with YieldCount=0 immediately enters sleep.
func TestAdaptivePoller_PureSleep(t *testing.T) {
	cfg := PollConfig{BusySpinCount: 0, YieldCount: 0, SleepThresholdUs: time.Hour}
	p := NewAdaptivePoller(cfg)

	if got := p.NextAction(); got != ActionSleep {
		t.Fatalf("first action: got %v want ActionSleep (pure sleep mode broken)", got)
	}
	if p.InPhase() != "sleep" {
		t.Fatalf("phase: got %q want sleep", p.InPhase())
	}
}

// TestAdaptivePoller_ResetReturnsToBusy validates that after consuming work
// the poller can be reset to phase 1 for the next idle stretch.
func TestAdaptivePoller_ResetReturnsToBusy(t *testing.T) {
	cfg := PollConfig{BusySpinCount: 2, YieldCount: 2, SleepThresholdUs: time.Hour}
	p := NewAdaptivePoller(cfg)

	// Drive to sleep.
	for i := 0; i < 10; i++ {
		_ = p.NextAction()
	}
	if p.InPhase() != "sleep" {
		t.Fatalf("expected sleep before reset, got %q", p.InPhase())
	}
	p.Reset()
	if p.InPhase() != "busy" {
		t.Fatalf("after reset: got %q want busy", p.InPhase())
	}
	if got := p.NextAction(); got != ActionContinue {
		t.Fatalf("after reset: got %v want ActionContinue", got)
	}
}

// TestAdaptivePoller_ZeroYieldCountSkipsToSleep ensures that a configuration
// with no yield budget cleanly transitions busy → sleep without spurious
// Yield actions, even when SleepThresholdUs is large.
func TestAdaptivePoller_ZeroYieldCountSkipsToSleep(t *testing.T) {
	cfg := PollConfig{BusySpinCount: 3, YieldCount: 0, SleepThresholdUs: time.Hour}
	p := NewAdaptivePoller(cfg)

	for i := 0; i < cfg.BusySpinCount; i++ {
		if got := p.NextAction(); got != ActionContinue {
			t.Fatalf("busy %d: got %v", i, got)
		}
	}
	if got := p.NextAction(); got != ActionSleep {
		t.Fatalf("after busy with zero yield: got %v want ActionSleep", got)
	}
}

// TestAdaptivePoller_DefaultsHaveSaneShape sanity-checks the spec defaults.
func TestAdaptivePoller_DefaultsHaveSaneShape(t *testing.T) {
	c := DefaultPollConfig
	if c.BusySpinCount <= 0 {
		t.Errorf("default BusySpinCount=%d, expected positive", c.BusySpinCount)
	}
	if c.YieldCount <= 0 {
		t.Errorf("default YieldCount=%d, expected positive", c.YieldCount)
	}
	if c.SleepThresholdUs <= 0 {
		t.Errorf("default SleepThresholdUs=%s, expected positive", c.SleepThresholdUs)
	}
	if c.SleepThresholdUs < time.Microsecond {
		t.Errorf("default SleepThresholdUs=%s, suspiciously small", c.SleepThresholdUs)
	}
}

func TestPollAction_String(t *testing.T) {
	cases := map[PollAction]string{
		ActionContinue: "continue",
		ActionYield:    "yield",
		ActionSleep:    "sleep",
	}
	for action, want := range cases {
		if got := action.String(); got != want {
			t.Errorf("PollAction(%d).String()=%q want %q", action, got, want)
		}
	}
}
