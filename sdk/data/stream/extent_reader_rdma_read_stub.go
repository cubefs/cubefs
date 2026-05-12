//go:build !(linux && rdma)

package stream

// One-sided RDMA Read stubs for non-RDMA builds. The build-tag-free
// extent_reader.go calls these from its read-path fast-path probe;
// on builds without RDMA the methods short-circuit so the existing
// two-sided + TCP fallback path runs unchanged.

func (reader *ExtentReader) tryReadViaRDMARead(_ string, _ *Packet, _ *ExtentRequest, _, _ int) (int, error) {
	// 0 bytes + nil error tells the caller "no fast path available"
	// — proceed to the next fallback rather than treating this as a
	// failure to invalidate caches over.
	return 0, nil
}

func invalidateExtentMRCache(_ string, _, _ uint64) {}

// PhaseAStatsSnapshot has moved to phase_a_counters.go (build-tag-free)
// so both the RDMA path and this stub can share one definition. Kept
// here only as a comment marker — if someone re-adds a build-tagged
// declaration of the same name the compiler will reject it.
