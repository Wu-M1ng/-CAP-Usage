package main

import (
	"testing"
	"time"
)

func trackedTestRecord(id string, chunkIndex int, output int64) UsageRecord {
	return UsageRecord{
		Provider:      "openai-compatible-test",
		Model:         "gpt-5.6",
		Alias:         "gpt-5.6",
		RequestedAt:   time.Now(),
		correlationID: id,
		Detail: UsageDetail{
			InputTokens:  100,
			OutputTokens: output,
			TotalTokens:  100 + output,
		},
		Latency: time.Duration(chunkIndex) * time.Millisecond,
	}
}

func TestStreamTrackerCommitsLatestUsageAtTerminal(t *testing.T) {
	tracker := newStreamUsageTracker()
	fallbacks := newUsageFallbackCoordinator()
	statistics := NewRequestStatistics()
	defer tracker.close()
	defer fallbacks.Flush()

	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-a", 1, 2), 1, false)
	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-a", 2, 9), 2, false)
	tracker.Observe(statistics, fallbacks, UsageRecord{correlationID: "response-a"}, 3, true)
	fallbacks.Flush()

	snapshot := statistics.Snapshot()
	if snapshot.TotalRequests != 1 || snapshot.OutputTokens != 9 || snapshot.TotalTokens != 109 {
		t.Fatalf("tracker snapshot = %#v, want latest terminal usage once", snapshot)
	}
}

func TestStreamTrackerHistoryOnlyTerminalCommitsOnce(t *testing.T) {
	tracker := newStreamUsageTracker()
	fallbacks := newUsageFallbackCoordinator()
	statistics := NewRequestStatistics()
	defer tracker.close()
	defer fallbacks.Flush()

	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-history", 4, 7), 4, false)
	tracker.Observe(statistics, fallbacks, UsageRecord{correlationID: "response-history"}, 5, true)
	fallbacks.Flush()
	if got := statistics.Snapshot().TotalRequests; got != 1 {
		t.Fatalf("history-only terminal records = %d, want one", got)
	}
}

func TestStreamTrackerConcurrentEqualTokenStreamsStaySeparate(t *testing.T) {
	tracker := newStreamUsageTracker()
	fallbacks := newUsageFallbackCoordinator()
	statistics := NewRequestStatistics()
	defer tracker.close()
	defer fallbacks.Flush()

	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-a", 1, 2), 1, false)
	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-b", 1, 2), 1, false)
	tracker.Observe(statistics, fallbacks, UsageRecord{correlationID: "response-a"}, 2, true)
	tracker.Observe(statistics, fallbacks, UsageRecord{correlationID: "response-b"}, 2, true)
	fallbacks.Flush()

	if got := statistics.Snapshot().TotalRequests; got != 2 {
		t.Fatalf("equal-token stream records = %d, want two response IDs", got)
	}
}

func TestStreamTrackerRejectsStaleChunkIndex(t *testing.T) {
	tracker := newStreamUsageTracker()
	fallbacks := newUsageFallbackCoordinator()
	statistics := NewRequestStatistics()
	defer tracker.close()
	defer fallbacks.Flush()

	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-stale", 7, 8), 7, false)
	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-stale", 3, 1), 3, false)
	tracker.Observe(statistics, fallbacks, UsageRecord{correlationID: "response-stale"}, 8, true)
	fallbacks.Flush()

	snapshot := statistics.Snapshot()
	if snapshot.OutputTokens != 8 {
		t.Fatalf("stale chunk replaced final usage: %#v", snapshot)
	}
}

func TestStreamTrackerExpiresIdleEntry(t *testing.T) {
	tracker := newStreamUsageTracker()
	fallbacks := newUsageFallbackCoordinator()
	statistics := NewRequestStatistics()
	defer tracker.close()
	defer fallbacks.Flush()

	tracker.Observe(statistics, fallbacks, trackedTestRecord("response-idle", 1, 6), 1, false)
	tracker.flushExpired(time.Now().Add(streamStateTTL + time.Second))
	fallbacks.Flush()
	if got := statistics.Snapshot().TotalRequests; got != 1 {
		t.Fatalf("expired stream records = %d, want one TTL fallback", got)
	}
}

func TestStreamTrackerCapacityIsBounded(t *testing.T) {
	tracker := newStreamUsageTracker()
	defer tracker.close()
	for i := 0; i < maxTrackedStreams+128; i++ {
		tracker.Observe(nil, nil, trackedTestRecord("response-"+string(rune(i)), 1, 1), 1, false)
	}
	tracker.mu.Lock()
	entries, bytes := len(tracker.entries), tracker.bytes
	tracker.mu.Unlock()
	if entries > maxTrackedStreams || bytes > maxTrackedStreamBytes {
		t.Fatalf("tracker bounds = entries %d bytes %d, want <= %d/%d", entries, bytes, maxTrackedStreams, maxTrackedStreamBytes)
	}
}
