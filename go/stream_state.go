package main

import (
	"strings"
	"sync"
	"time"
)

const (
	maxTrackedStreams     = 2048
	maxTrackedStreamBytes = 8 << 20
	// A stream can legitimately last tens of seconds while emitting running
	// usage snapshots. Expiring after five seconds would commit an intermediate
	// total before the terminal response arrives and could undercount tokens.
	streamStateTTL = 60 * time.Second
)

type trackedStream struct {
	key        string
	record     UsageRecord
	chunkIndex int
	updatedAt  time.Time
	bytes      int
	stats      *RequestStatistics
	fallbacks  *usageFallbackCoordinator
}

// streamUsageTracker keeps only the latest compact UsageRecord for each
// response/request identity. It never retains callback JSON or response body.
type streamUsageTracker struct {
	mu      sync.Mutex
	entries map[string]trackedStream
	bytes   int
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	closed  bool
}

func newStreamUsageTracker() *streamUsageTracker {
	tracker := &streamUsageTracker{
		entries: make(map[string]trackedStream),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go tracker.run()
	return tracker
}

func (t *streamUsageTracker) Observe(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, record UsageRecord, chunkIndex int, terminal bool) {
	if t == nil || strings.TrimSpace(record.correlationID) == "" {
		if record.Detail != (UsageDetail{}) {
			t.commit(statistics, fallbacks, record)
		}
		return
	}
	key := usageFallbackStreamKey(record)
	if key == "" {
		t.commit(statistics, fallbacks, record)
		return
	}
	now := time.Now()
	if terminal {
		var final UsageRecord
		entryKey := key
		t.mu.Lock()
		previous, exists := t.entries[entryKey]
		if !exists {
			entryKey, previous, exists = t.findByCorrelationLocked(record.correlationID)
		}
		if exists {
			if record.Detail == (UsageDetail{}) {
				final = previous.record
			} else if chunkIndex > 0 && previous.chunkIndex > chunkIndex {
				final = previous.record
			} else {
				final = record
			}
			t.removeLocked(entryKey)
		} else {
			final = record
		}
		t.mu.Unlock()
		if final.Detail != (UsageDetail{}) {
			t.commit(statistics, fallbacks, final)
		}
		return
	}
	if record.Detail == (UsageDetail{}) {
		return
	}
	recordBytes := usageFallbackRecordBytes(record)
	if recordBytes > maxTrackedStreamBytes {
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	if previous, exists := t.entries[key]; exists {
		if chunkIndex > 0 && previous.chunkIndex > chunkIndex {
			t.mu.Unlock()
			return
		}
		t.bytes -= previous.bytes
	}
	for len(t.entries) >= maxTrackedStreams || t.bytes+recordBytes > maxTrackedStreamBytes {
		if !t.evictOldestLocked() {
			break
		}
	}
	if len(t.entries) >= maxTrackedStreams || t.bytes+recordBytes > maxTrackedStreamBytes {
		t.mu.Unlock()
		return
	}
	t.entries[key] = trackedStream{
		key:        key,
		record:     record,
		chunkIndex: chunkIndex,
		updatedAt:  now,
		bytes:      recordBytes,
		stats:      usageFallbackStats(statistics),
		fallbacks:  fallbacks,
	}
	t.bytes += recordBytes
	t.mu.Unlock()
	t.signal()
}

func (t *streamUsageTracker) findByCorrelationLocked(correlationID string) (string, trackedStream, bool) {
	correlationID = strings.TrimSpace(correlationID)
	if correlationID == "" {
		return "", trackedStream{}, false
	}
	for key, entry := range t.entries {
		if strings.TrimSpace(entry.record.correlationID) == correlationID {
			return key, entry, true
		}
	}
	return "", trackedStream{}, false
}

func (t *streamUsageTracker) commit(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, record UsageRecord) {
	if record.RequestedAt.IsZero() {
		record.RequestedAt = time.Now()
	}
	if fallbacks != nil {
		sanitizeUsageRecordForStats(statistics, &record)
		fallbacks.ScheduleForStats(statistics, record)
		return
	}
	if statistics != nil {
		sanitizeUsageRecordForStats(statistics, &record)
		statistics.Record(record)
	}
}

func (t *streamUsageTracker) signal() {
	if t == nil {
		return
	}
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *streamUsageTracker) run() {
	if t == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(t.done)
	for {
		select {
		case <-ticker.C:
			t.flushExpired(time.Now())
		case <-t.wake:
		case <-t.stop:
			t.flushExpired(time.Now())
			return
		}
	}
}

func (t *streamUsageTracker) flushExpired(now time.Time) {
	if t == nil {
		return
	}
	var expired []trackedStream
	t.mu.Lock()
	if !t.closed {
		for key, entry := range t.entries {
			if now.Sub(entry.updatedAt) >= streamStateTTL {
				expired = append(expired, entry)
				delete(t.entries, key)
				t.bytes -= entry.bytes
			}
		}
	}
	if t.bytes < 0 {
		t.bytes = 0
	}
	t.mu.Unlock()
	for _, entry := range expired {
		t.commit(entry.stats, entry.fallbacks, entry.record)
	}
}

func (t *streamUsageTracker) removeLocked(key string) {
	entry, ok := t.entries[key]
	if !ok {
		return
	}
	delete(t.entries, key)
	t.bytes -= entry.bytes
	if t.bytes < 0 {
		t.bytes = 0
	}
}

func (t *streamUsageTracker) evictOldestLocked() bool {
	var oldestKey string
	var oldest time.Time
	for key, entry := range t.entries {
		if oldestKey == "" || entry.updatedAt.Before(oldest) {
			oldestKey = key
			oldest = entry.updatedAt
		}
	}
	if oldestKey == "" {
		return false
	}
	t.removeLocked(oldestKey)
	return true
}

func (t *streamUsageTracker) close() {
	if t == nil {
		return
	}
	var pending []trackedStream
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		for key, entry := range t.entries {
			pending = append(pending, entry)
			delete(t.entries, key)
		}
		t.bytes = 0
		close(t.stop)
	}
	t.mu.Unlock()
	<-t.done
	for _, entry := range pending {
		t.commit(entry.stats, entry.fallbacks, entry.record)
	}
}
