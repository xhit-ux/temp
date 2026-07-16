package metrics

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker maintains live counters for monitoring write throughput and errors.
// All counters are safe for concurrent use.
type Tracker struct {
	// Event counters
	totalReceived          atomic.Int64
	totalWritten           atomic.Int64
	totalFailed            atomic.Int64
	totalDropped           atomic.Int64
	totalAmbiguousResolved atomic.Int64 // 23505 unique_violation resolved as success
	totalDeadLetter        atomic.Int64 // events written to dead-letter files
	totalDeadReplayed      atomic.Int64 // events successfully replayed from dead-letter
	currentInQueue         atomic.Int64

	// Batch counters
	batchesWritten atomic.Int64
	batchesFailed  atomic.Int64
	batchesActive  atomic.Int64

	// Write latency tracking
	totalWriteNanos atomic.Int64
	maxWriteNanos   atomic.Int64
	writeLatencies   *sync.Map // batch_id -> start time for in-flight tracking

	// Snapshot for rate calculation
	mu           sync.RWMutex
	lastSnapshot struct {
		time    time.Time
		written int64
	}
	ratePerSecond float64
}

// NewTracker creates a new metrics tracker.
func NewTracker() *Tracker {
	return &Tracker{
		writeLatencies: &sync.Map{},
	}
}

// RecordReceived increments the received event counter and queue depth.
func (t *Tracker) RecordReceived(count int64) {
	t.totalReceived.Add(count)
	t.currentInQueue.Add(count)
}

// RecordDequeued decrements the queue depth counter.
func (t *Tracker) RecordDequeued(count int64) {
	t.currentInQueue.Add(-count)
}

// RecordBatchStart marks a batch as in-flight.
func (t *Tracker) RecordBatchStart(batchID string) {
	t.batchesActive.Add(1)
	t.writeLatencies.Store(batchID, time.Now())
}

// RecordBatchSuccess marks a batch as successfully written.
func (t *Tracker) RecordBatchSuccess(batchID string, eventCount int64) {
	t.batchesActive.Add(-1)
	t.batchesWritten.Add(1)
	t.totalWritten.Add(eventCount)
	t.currentInQueue.Add(-eventCount)

	// Track write latency
	if startVal, ok := t.writeLatencies.LoadAndDelete(batchID); ok {
		start := startVal.(time.Time)
		elapsed := time.Since(start)
		nanos := elapsed.Nanoseconds()
		t.totalWriteNanos.Add(nanos)
		// Update max atomically
		for {
			current := t.maxWriteNanos.Load()
			if nanos <= current || t.maxWriteNanos.CompareAndSwap(current, nanos) {
				break
			}
		}
	}

	t.updateRate()
}

// RecordBatchFailure marks a batch as failed.
func (t *Tracker) RecordBatchFailure(batchID string) {
	t.batchesActive.Add(-1)
	t.batchesFailed.Add(1)
	t.writeLatencies.Delete(batchID)

	t.updateRate()
}

// RecordFailed increments the failed event counter.
func (t *Tracker) RecordFailed(count int64) {
	t.totalFailed.Add(count)
	t.currentInQueue.Add(-count)
}

// RecordDropped increments the dropped event counter.
func (t *Tracker) RecordDropped(count int64) {
	t.totalDropped.Add(count)
}

// RecordAmbiguousResolved handles a batch that triggered SQLSTATE 23505
// (unique_violation).  The data was already persisted by a previous attempt
// whose acknowledgement was lost — the original write contribution happened
// in that previous time window, so totalAmbiguousResolved is counted
// separately and NOT added to totalWritten, avoiding rate distortion.
func (t *Tracker) RecordAmbiguousResolved(batchID string, eventCount int64) {
	t.batchesActive.Add(-1)
	t.batchesWritten.Add(1)
	t.totalAmbiguousResolved.Add(eventCount)
	t.currentInQueue.Add(-eventCount)

	// Track write latency (same as RecordBatchSuccess)
	if startVal, ok := t.writeLatencies.LoadAndDelete(batchID); ok {
		start := startVal.(time.Time)
		elapsed := time.Since(start)
		nanos := elapsed.Nanoseconds()
		t.totalWriteNanos.Add(nanos)
		for {
			current := t.maxWriteNanos.Load()
			if nanos <= current || t.maxWriteNanos.CompareAndSwap(current, nanos) {
				break
			}
		}
	}

	t.updateRate()
}

// updateRate recalculates the per-second write rate.
func (t *Tracker) updateRate() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastSnapshot.time).Seconds()
	if elapsed >= 1.0 {
		written := t.totalWritten.Load()
		delta := written - t.lastSnapshot.written
		t.ratePerSecond = float64(delta) / elapsed
		t.lastSnapshot.time = now
		t.lastSnapshot.written = written
	}
}

// RatePerSecond returns the estimated write rate in events/second.
func (t *Tracker) RatePerSecond() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ratePerSecond
}

// AverageWriteLatency returns the average batch write latency.
func (t *Tracker) AverageWriteLatency() time.Duration {
	batches := t.batchesWritten.Load()
	if batches == 0 {
		return 0
	}
	return time.Duration(t.totalWriteNanos.Load() / batches)
}

// MaxWriteLatency returns the maximum batch write latency observed.
func (t *Tracker) MaxWriteLatency() time.Duration {
	return time.Duration(t.maxWriteNanos.Load())
}

// RecordDeadLetter increments the dead-letter event counter (events sent to DLQ).
func (t *Tracker) RecordDeadLetter(count int64) {
	t.totalDeadLetter.Add(count)
}

// RecordDeadReplayed increments the dead-letter replayed counter.
func (t *Tracker) RecordDeadReplayed(count int64) {
	t.totalDeadReplayed.Add(count)
}

// Snapshot returns a point-in-time snapshot of all metrics.
type Snapshot struct {
	TotalReceived          int64
	TotalWritten           int64
	TotalFailed            int64
	TotalDropped           int64
	TotalAmbiguousResolved int64
	TotalDeadLetter        int64
	TotalDeadReplayed      int64
	CurrentInQueue         int64
	BatchesWritten         int64
	BatchesFailed          int64
	BatchesActive          int64
	RatePerSecond          float64
	AvgWriteLatency        string
	MaxWriteLatency        string
}

// Snapshot returns the current metric values.
func (t *Tracker) Snapshot() Snapshot {
	return Snapshot{
		TotalReceived:          t.totalReceived.Load(),
		TotalWritten:           t.totalWritten.Load(),
		TotalFailed:            t.totalFailed.Load(),
		TotalDropped:           t.totalDropped.Load(),
		TotalAmbiguousResolved: t.totalAmbiguousResolved.Load(),
		TotalDeadLetter:        t.totalDeadLetter.Load(),
		TotalDeadReplayed:      t.totalDeadReplayed.Load(),
		CurrentInQueue:         t.currentInQueue.Load(),
		BatchesWritten:         t.batchesWritten.Load(),
		BatchesFailed:          t.batchesFailed.Load(),
		BatchesActive:          t.batchesActive.Load(),
		RatePerSecond:          t.RatePerSecond(),
		AvgWriteLatency:        t.AverageWriteLatency().String(),
		MaxWriteLatency:        t.MaxWriteLatency().String(),
	}
}

// LogReport prints a periodic summary of metrics.
func (t *Tracker) LogReport() {
	snap := t.Snapshot()
	log.Printf(
		"[Metrics] received=%d written=%d failed=%d dropped=%d ambig=%d dlq=%d dlq_rpl=%d queue=%d rate=%.1f/s batches(ok=%d fail=%d active=%d) avgLat=%s maxLat=%s",
		snap.TotalReceived,
		snap.TotalWritten,
		snap.TotalFailed,
		snap.TotalDropped,
		snap.TotalAmbiguousResolved,
		snap.TotalDeadLetter,
		snap.TotalDeadReplayed,
		snap.CurrentInQueue,
		snap.RatePerSecond,
		snap.BatchesWritten,
		snap.BatchesFailed,
		snap.BatchesActive,
		snap.AvgWriteLatency,
		snap.MaxWriteLatency,
	)
}

// UpdateDeadLetterStats scans the dead-letter directory and updates file count/size stats.
// This is a lightweight call that doesn't read file contents.
func (t *Tracker) UpdateDeadLetterStats(_ string) {
	// stats are read by the deadletter.Writer.Stats() method and logged separately
}

// StartPeriodicReport starts a goroutine that prints metrics at the given interval.
func (t *Tracker) StartPeriodicReport(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			t.LogReport()
		}
	}()
}
