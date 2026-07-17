package writer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	"opc2ymatrix/deadletter"
	"opc2ymatrix/metrics"
	"opc2ymatrix/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrAmbiguousCommit is returned by copyFrom when the database reports a
// unique_violation (SQLSTATE 23505).  This means the batch was already
// committed on a previous attempt whose acknowledgement was lost in the
// network — the data is safe, and no retry or dead-letter is needed.
var ErrAmbiguousCommit = errors.New("ambiguous commit resolved: batch already exists (SQLSTATE 23505)")

// BatchWriter reads events from a channel, accumulates them into batches,
// and writes them to YMatrix using pgx CopyFrom with retry logic.
// Supports both normal (batch) and priority (single-row immediate) paths.
type BatchWriter struct {
	eventCh    <-chan model.IngestEvent
	db         *pgx.Conn // dedicated connection for CopyFrom
	metrics    *metrics.Tracker
	dlq        *deadletter.Writer
	cfg        BatchConfig
	flushTimer *time.Timer
	stopCh     chan struct{}
	doneCh     chan struct{}
	// isPriority distinguishes the priority channel writer from normal batch writers.
	isPriority bool
}

// BatchConfig holds configuration for batch accumulation and retry.
type BatchConfig struct {
	BatchSize       int // max events per CopyFrom batch (unused by priority writer)
	FlushInterval   time.Duration
	MaxRetries      int
	RetryBaseDelay  time.Duration
	WriterPoolSize  int
}

// DefaultBatchConfig returns sensible defaults that can be overridden by env.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:      500,
		FlushInterval:  5 * time.Second,
		MaxRetries:     5,
		RetryBaseDelay: 1 * time.Second,
		WriterPoolSize: 2,
	}
}

// NewBatchWriter creates a normal batch writer worker.
func NewBatchWriter(
	eventCh <-chan model.IngestEvent,
	dbConn *pgx.Conn,
	m *metrics.Tracker,
	dlq *deadletter.Writer,
	cfg BatchConfig,
) *BatchWriter {
	return &BatchWriter{
		eventCh:    eventCh,
		db:         dbConn,
		metrics:    m,
		dlq:        dlq,
		cfg:        cfg,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		isPriority: false,
	}
}

// NewPriorityWriter creates a priority writer that writes events immediately
// (single-row transactions) without batch accumulation.  Used for abnormal/alert events.
func NewPriorityWriter(
	eventCh <-chan model.IngestEvent,
	dbConn *pgx.Conn,
	m *metrics.Tracker,
	dlq *deadletter.Writer,
	cfg BatchConfig,
) *BatchWriter {
	return &BatchWriter{
		eventCh:    eventCh,
		db:         dbConn,
		metrics:    m,
		dlq:        dlq,
		cfg:        cfg,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		isPriority: true,
	}
}

// Start begins the batch accumulation and flush loop (or immediate write loop for priority).
func (bw *BatchWriter) Start(ctx context.Context) {
	defer close(bw.doneCh)

	if bw.isPriority {
		bw.startPriority(ctx)
		return
	}
	bw.startNormal(ctx)
}

func (bw *BatchWriter) startNormal(ctx context.Context) {
	batch := make([]model.IngestEvent, 0, bw.cfg.BatchSize)
	bw.flushTimer = time.NewTimer(bw.cfg.FlushInterval)

	log.Printf("[Writer] Normal writer started (batch_size=%d, flush_interval=%s, max_retries=%d)",
		bw.cfg.BatchSize, bw.cfg.FlushInterval, bw.cfg.MaxRetries)

	for {
		select {
		case <-bw.stopCh:
			if len(batch) > 0 {
				log.Printf("[Writer] Stopping: flushing %d remaining events", len(batch))
				bw.flushNormal(ctx, batch)
			}
			return

		case event, ok := <-bw.eventCh:
			if !ok {
				if len(batch) > 0 {
					log.Printf("[Writer] Channel closed: flushing %d remaining events", len(batch))
					bw.flushNormal(ctx, batch)
				}
				return
			}

			batch = append(batch, event)

			if len(batch) >= bw.cfg.BatchSize {
				bw.flushTimer.Stop()
				bw.flushNormal(ctx, batch)
				batch = make([]model.IngestEvent, 0, bw.cfg.BatchSize)
				bw.flushTimer.Reset(bw.cfg.FlushInterval)
			}

		case <-bw.flushTimer.C:
			if len(batch) > 0 {
				bw.flushNormal(ctx, batch)
				batch = make([]model.IngestEvent, 0, bw.cfg.BatchSize)
			}
			bw.flushTimer.Reset(bw.cfg.FlushInterval)
		}
	}
}

func (bw *BatchWriter) startPriority(ctx context.Context) {
	log.Printf("[Writer] Priority writer started (immediate single-row writes, max_retries=%d)", bw.cfg.MaxRetries)

	for {
		select {
		case <-bw.stopCh:
			// Priority writer processes events immediately, no batch to drain.
			return

		case event, ok := <-bw.eventCh:
			if !ok {
				return
			}
			bw.flushPriority(ctx, event)
		}
	}
}

// flushNormal writes a normal batch using CopyFrom with retry.
func (bw *BatchWriter) flushNormal(ctx context.Context, batch []model.IngestEvent) {
	if len(batch) == 0 {
		return
	}

	batchID := uuid.New().String()
	bw.metrics.RecordBatchStart(batchID)

	err := bw.writeNormalBatchWithRetry(ctx, batch, batchID)
	if errors.Is(err, ErrAmbiguousCommit) {
		bw.metrics.RecordAmbiguousResolved(batchID, int64(len(batch)))
		log.Printf("[Writer] INFO batch %s: %d events — ambiguous commit resolved (data already persisted, SQLSTATE 23505)",
			batchID, len(batch))
		return
	}
	if err != nil {
		bw.metrics.RecordBatchFailure(batchID)
		bw.metrics.RecordFailed(int64(len(batch)))
		log.Printf("[Writer] ERROR batch %s: %d events failed after %d retries: %v",
			batchID, len(batch), bw.cfg.MaxRetries, err)
		// Write to dead letter (batch split into per-event records)
		if bw.dlq != nil {
			errClass := errorClass(err)
			if dlqErr := bw.dlq.WriteBatch("normal", batchID, batch, errClass, err.Error(), bw.cfg.MaxRetries); dlqErr != nil {
				log.Printf("[Writer] CRITICAL failed to write dead letter for batch %s: %v", batchID, dlqErr)
			} else {
				bw.metrics.RecordDeadLetter(int64(len(batch)))
			}
		}
		return
	}

	bw.metrics.RecordBatchSuccess(batchID, int64(len(batch)))
}

// flushPriority writes a single abnormal event immediately in its own transaction.
func (bw *BatchWriter) flushPriority(ctx context.Context, event model.IngestEvent) {
	batchID := uuid.New().String()
	bw.metrics.RecordBatchStart(batchID)

	err := bw.writeSingleWithRetry(ctx, event, batchID)
	if errors.Is(err, ErrAmbiguousCommit) {
		bw.metrics.RecordAmbiguousResolved(batchID, 1)
		log.Printf("[Writer] INFO prio batch %s: event %s — ambiguous commit resolved",
			batchID, event.EventID)
		return
	}
	if err != nil {
		bw.metrics.RecordBatchFailure(batchID)
		bw.metrics.RecordFailed(1)
		log.Printf("[Writer] ERROR prio batch %s: event %s failed after %d retries: %v",
			batchID, event.EventID, bw.cfg.MaxRetries, err)
		// Write to dead letter
		if bw.dlq != nil {
			errClass := errorClass(err)
			if dlqErr := bw.dlq.WriteSingle("priority", batchID, event, errClass, err.Error(), bw.cfg.MaxRetries); dlqErr != nil {
				log.Printf("[Writer] CRITICAL failed to write dead letter for prio event %s: %v", event.EventID, dlqErr)
			} else {
				bw.metrics.RecordDeadLetter(1)
			}
		}
		return
	}

	bw.metrics.RecordBatchSuccess(batchID, 1)
}

// errorClass returns "permanent" for non-retryable errors, "transient" otherwise.
func errorClass(err error) string {
	if !isRetryable(err) {
		return "permanent"
	}
	return "transient"
}

// writeNormalBatchWithRetry attempts CopyFrom with exponential backoff.
func (bw *BatchWriter) writeNormalBatchWithRetry(ctx context.Context, batch []model.IngestEvent, batchID string) error {
	var lastErr error

	for attempt := 0; attempt <= bw.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := bw.cfg.RetryBaseDelay * time.Duration(int64(math.Pow(2, float64(attempt-1))))
			jitter := time.Duration(rand.Int63n(int64(delay) / 4))
			sleepDuration := delay + jitter

			log.Printf("[Writer] Retry %d/%d after %s", attempt, bw.cfg.MaxRetries, sleepDuration)
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(sleepDuration):
			}
		}

		err := bw.copyFrom(ctx, batch, batchID)
		if err == nil {
			if attempt > 0 {
				log.Printf("[Writer] Retry %d succeeded for %d events", attempt, len(batch))
			}
			return nil
		}

		if errors.Is(err, ErrAmbiguousCommit) {
			return ErrAmbiguousCommit
		}

		lastErr = err

		if !isRetryable(err) {
			log.Printf("[Writer] Non-retryable error: %v", err)
			return fmt.Errorf("non-retryable: %w", err)
		}

		log.Printf("[Writer] Retryable error (attempt %d/%d): %v", attempt, bw.cfg.MaxRetries, err)
	}

	return fmt.Errorf("exhausted %d retries: %w", bw.cfg.MaxRetries, lastErr)
}

// writeSingleWithRetry writes one event in a single transaction with retry.
func (bw *BatchWriter) writeSingleWithRetry(ctx context.Context, event model.IngestEvent, batchID string) error {
	var lastErr error

	for attempt := 0; attempt <= bw.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := bw.cfg.RetryBaseDelay * time.Duration(int64(math.Pow(2, float64(attempt-1))))
			jitter := time.Duration(rand.Int63n(int64(delay) / 4))
			sleepDuration := delay + jitter

			log.Printf("[Writer] Priority retry %d/%d after %s", attempt, bw.cfg.MaxRetries, sleepDuration)
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(sleepDuration):
			}
		}

		err := bw.insertSingle(ctx, event, batchID)
		if err == nil {
			if attempt > 0 {
				log.Printf("[Writer] Priority retry %d succeeded for event %s", attempt, event.EventID)
			}
			return nil
		}

		if errors.Is(err, ErrAmbiguousCommit) {
			return ErrAmbiguousCommit
		}

		lastErr = err

		if !isRetryable(err) {
			log.Printf("[Writer] Priority non-retryable error: %v", err)
			return fmt.Errorf("non-retryable: %w", err)
		}

		log.Printf("[Writer] Priority retryable error (attempt %d/%d): %v", attempt, bw.cfg.MaxRetries, err)
	}

	return fmt.Errorf("exhausted %d retries: %w", bw.cfg.MaxRetries, lastErr)
}

// copyFrom performs a single pgx.CopyFrom batch insert.
// Validation is performed at ingest time (handler/ingest.go); no need to re-validate here.
func (bw *BatchWriter) copyFrom(ctx context.Context, batch []model.IngestEvent, batchID string) error {
	rows := make([][]interface{}, len(batch))
	for i := range batch {
		rows[i] = batch[i].Row(batchID)
	}

	_, err := bw.db.CopyFrom(
		ctx,
		pgx.Identifier{"opc_point_data"},
		[]string{
			"event_id", "device_id", "point_name", "data_type", "unit",
			"value_number", "value_text", "value_time",
			"quality_code", "quality_name", "event_type",
			"is_abnormal", "abnormal_reason",
			"source_timestamp", "server_timestamp", "collector_timestamp",
			"batch_id",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("CopyFrom %d rows: %w", len(rows), ErrAmbiguousCommit)
		}
		return fmt.Errorf("CopyFrom failed for %d rows: %w", len(rows), err)
	}

	return nil
}

// insertSingle writes one event via single-row INSERT ... ON CONFLICT DO NOTHING.
// Uses ON CONFLICT so that ambiguous-commit retries are handled idempotently.
func (bw *BatchWriter) insertSingle(ctx context.Context, event model.IngestEvent, batchID string) error {
	event.Validate()
	row := event.Row(batchID)

	_, err := bw.db.Exec(ctx,
		`INSERT INTO opc_point_data (
			event_id, device_id, point_name, data_type, unit,
			value_number, value_text, value_time,
			quality_code, quality_name, event_type,
			is_abnormal, abnormal_reason,
			source_timestamp, server_timestamp, collector_timestamp,
			batch_id
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11,
			$12, $13,
			$14, $15, $16,
			$17
		) ON CONFLICT (event_id) DO NOTHING`,
		row...,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("insertSingle event %s: %w", event.EventID, ErrAmbiguousCommit)
		}
		return fmt.Errorf("insertSingle failed for event_id=%s: %w", event.EventID, err)
	}
	return nil
}

// InsertOnConflict performs a batch INSERT ... ON CONFLICT DO NOTHING.
// This is intended for dead-letter compensation where duplicate event_id
// values may exist.  Not used on the normal CopyFrom path.
//
// TODO(perf): If dead-letter replay volume grows, switch to a single
// INSERT ... SELECT unnest($1::uuid[]) ... ON CONFLICT DO NOTHING
// instead of looping per row.
func (bw *BatchWriter) InsertOnConflict(ctx context.Context, batch []model.IngestEvent, batchID string) error {
	for _, event := range batch {
		event.Validate()
		row := event.Row(batchID)
		_, err := bw.db.Exec(ctx,
			`INSERT INTO opc_point_data (
				event_id, device_id, point_name, data_type, unit,
				value_number, value_text, value_time,
				quality_code, quality_name, event_type,
				is_abnormal, abnormal_reason,
				source_timestamp, server_timestamp, collector_timestamp,
				batch_id
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8,
				$9, $10, $11,
				$12, $13,
				$14, $15, $16,
				$17
			) ON CONFLICT (event_id) DO NOTHING`,
			row...,
		)
		if err != nil {
			return fmt.Errorf("InsertOnConflict failed for event_id=%s: %w", event.EventID, err)
		}
	}
	return nil
}

// Stop signals the writer to flush and exit.
func (bw *BatchWriter) Stop() {
	close(bw.stopCh)
	if bw.flushTimer != nil {
		bw.flushTimer.Stop()
	}
}

// Wait blocks until the writer has finished.
func (bw *BatchWriter) Wait() {
	<-bw.doneCh
}

// isRetryable determines whether a database error is worth retrying.
// Uses PgError SQLSTATE classification when available, falls back to string matching.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for pgconn.PgError with known retryable SQLSTATE codes
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// Class 08 — Connection Exception
		case "08000", "08003", "08006", "08001", "08004":
			return true
		// Class 40 — Transaction Rollback (deadlock, serialization)
		case "40001", "40P01":
			return true
		// Class 53 — Insufficient Resources
		case "53300", "53400":
			return true
		// Class 57 — Operator Intervention
		case "57P01", "57P02", "57P03":
			return true
		// Class 25 — Invalid Transaction State
		case "25001", "25P02":
			return true
		default:
			return false
		}
	}

	// Fallback: string-based matching for non-PgError errors (e.g. connection errors)
	errStr := err.Error()
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"connection closed",
		"timeout",
		"too many clients",
		"server closed the connection",
		"i/o timeout",
		"broken pipe",
		"connection pool exhausted",
		"conn busy",
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}
