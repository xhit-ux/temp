package deadletter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"opc2ymatrix/metrics"
	"opc2ymatrix/model"
)

// ReplayFunc is the callback that writes a batch of recovered events to the database.
// Uses InsertOnConflict() for idempotency (Section 11.3).
type ReplayFunc func(ctx context.Context, batch []model.IngestEvent, batchID string) error

// Replayer periodically scans the dead-letter directory for NDJSON files,
// reads events in small batches, and replays them via InsertOnConflict.
type Replayer struct {
	dir        string
	interval   time.Duration
	batchSize  int
	dlWriter   *Writer
	metrics    *metrics.Tracker
	replayFn   ReplayFunc
	stopCh     chan struct{}
	doneCh     chan struct{}
}

// NewReplayer creates a periodic dead-letter replayer.
func NewReplayer(
	dir string,
	interval time.Duration,
	batchSize int,
	m *metrics.Tracker,
	fn ReplayFunc,
) (*Replayer, error) {
	dlw, err := NewWriter(dir) // reuse Writer for Dir management (not for writing)
	if err != nil {
		return nil, err
	}
	return &Replayer{
		dir:       dir,
		interval:  interval,
		batchSize: batchSize,
		dlWriter:  dlw,
		metrics:   m,
		replayFn:  fn,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}, nil
}

// Start begins the periodic replay loop.
func (r *Replayer) Start(ctx context.Context) {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	log.Printf("[DeadLetter] Replayer started (interval=%s, batch_size=%d)", r.interval, r.batchSize)

	for {
		select {
		case <-r.stopCh:
			log.Println("[DeadLetter] Replayer stopped")
			return
		case <-ctx.Done():
			log.Println("[DeadLetter] Replayer cancelled")
			return
		case <-ticker.C:
			r.processFiles(ctx)
		}
	}
}

// Stop signals the replayer to exit.
func (r *Replayer) Stop() {
	close(r.stopCh)
}

// Wait blocks until the replayer has finished.
func (r *Replayer) Wait() {
	<-r.doneCh
}

// ReplayNow triggers an immediate replay of all current dead-letter files.
// Used by the admin manual-trigger endpoint.
func (r *Replayer) ReplayNow(ctx context.Context) (replayed int, failed int, err error) {
	return r.processFiles(ctx)
}

// processFiles scans all .ndjson files, reads events in batches, and replays them.
// Successfully replayed events are removed from the file; failed events remain.
func (r *Replayer) processFiles(ctx context.Context) (replayed int, failed int, err error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return 0, 0, fmt.Errorf("read dead-letter dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ndjson" {
			continue
		}

		filePath := filepath.Join(r.dir, entry.Name())
		rep, fail, err := r.replayFile(ctx, filePath)
		replayed += rep
		failed += fail
		if err != nil {
			log.Printf("[DeadLetter] ERROR processing %s: %v", filePath, err)
		}
	}

	if replayed+failed > 0 {
		log.Printf("[DeadLetter] Replay cycle: replayed=%d failed=%d", replayed, failed)
	}

	r.metrics.UpdateDeadLetterStats(r.dir)
	return replayed, failed, nil
}

// replayFile reads one NDJSON file, replays events, and removes successfully replayed lines.
func (r *Replayer) replayFile(ctx context.Context, filePath string) (replayed int, failed int, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var remaining []Record
	batch := make([]model.IngestEvent, 0, r.batchSize)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			log.Printf("[DeadLetter] WARNING skipping unparseable line in %s: %v", filePath, err)
			continue
		}

		batch = append(batch, rec.Event)

		// Flush batch when it reaches target size
		if len(batch) >= r.batchSize {
			err := r.replayFn(ctx, batch, rec.BatchID)
			if err != nil {
				log.Printf("[DeadLetter] ERROR replay batch failed (batch %s, %d events): %v",
					rec.BatchID, len(batch), err)
				failed += len(batch)
				remaining = append(remaining, batchToRecords(batch, rec)...)
			} else {
				replayed += len(batch)
			}
			batch = batch[:0]
		}
	}

	// Flush remainder
	if len(batch) > 0 {
		// Use dummy batchID for last partial batch
		err := r.replayFn(ctx, batch, "dlq-replay-"+time.Now().Format(time.RFC3339))
		if err != nil {
			log.Printf("[DeadLetter] ERROR replay final batch failed (%d events): %v", len(batch), err)
			failed += len(batch)
			remaining = append(remaining, batchToRecords(batch, Record{Path: "normal"})...)
		} else {
			replayed += len(batch)
		}
	}

	if err := scanner.Err(); err != nil {
		return replayed, failed, fmt.Errorf("scanner error: %w", err)
	}

	// Overwrite file with remaining (failed) records, or delete if all succeeded
	if len(remaining) > 0 {
		if err := r.rewriteFile(filePath, remaining); err != nil {
			return replayed, failed, fmt.Errorf("rewrite remaining: %w", err)
		}
	} else {
		// All records replayed successfully — remove the file
		if err := os.Remove(filePath); err != nil {
			log.Printf("[DeadLetter] WARNING could not remove %s: %v", filePath, err)
		} else {
			log.Printf("[DeadLetter] All records replayed, removed %s", filePath)
		}
	}

	return replayed, failed, nil
}

// rewriteFile writes a new file with the remaining records, then atomically replaces the original.
func (r *Replayer) rewriteFile(filePath string, records []Record) error {
	tmpPath := filePath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	enc := json.NewEncoder(tmpFile)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("encode record: %w", err)
		}
	}
	tmpFile.Close()

	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp to original: %w", err)
	}

	return nil
}

func batchToRecords(batch []model.IngestEvent, template Record) []Record {
	records := make([]Record, 0, len(batch))
	for _, event := range batch {
		r := template
		r.Event = event
		records = append(records, r)
	}
	return records
}