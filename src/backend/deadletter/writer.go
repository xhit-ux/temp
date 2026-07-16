package deadletter

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"opc2ymatrix/model"
)

// Record represents a single dead-letter entry written to NDJSON.
// Conforms to Section 11.2 of the V2.0 PoC document.
type Record struct {
	Path         string            `json:"path"`          // "normal" or "priority"
	BatchID      string            `json:"batch_id"`
	Event        model.IngestEvent `json:"event"`
	ErrorClass   string            `json:"error_class"`   // "transient" or "permanent"
	ErrorMessage string            `json:"error_message"`
	RetryCount   int               `json:"retry_count"`
	FirstFailedAt string           `json:"first_failed_at"`
	LastFailedAt  string           `json:"last_failed_at"`
}

// Writer appends failed events to date-rolled NDJSON files.
// Thread-safe (protected by a mutex for file writes).
type Writer struct {
	dir  string
	mu   sync.Mutex
	file *os.File
	date string
}

// NewWriter creates a dead-letter writer that stores files in the given directory.
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dead-letter dir %s: %w", dir, err)
	}
	return &Writer{dir: dir}, nil
}

// WriteRecords writes a batch of dead-letter records.
// Each record corresponds to one failed event (normal batches are split into per-event records).
func (w *Writer) WriteRecords(records []Record) error {
	if len(records) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := w.getOrOpenFile()
	if err != nil {
		return err
	}

	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			log.Printf("[DeadLetter] ERROR marshaling record for event %s: %v", rec.Event.EventID, err)
			continue
		}
		data = append(data, '\n')
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("write dead-letter record: %w", err)
		}
	}

	return nil
}

// WriteBatch converts a failed normal batch into per-event dead-letter records and writes them.
func (w *Writer) WriteBatch(path string, batchID string, batch []model.IngestEvent, errClass string, errMsg string, retryCount int) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	records := make([]Record, 0, len(batch))
	for _, event := range batch {
		records = append(records, Record{
			Path:         path,
			BatchID:      batchID,
			Event:        event,
			ErrorClass:   errClass,
			ErrorMessage: errMsg,
			RetryCount:   retryCount,
			FirstFailedAt: now,
			LastFailedAt:  now,
		})
	}
	return w.WriteRecords(records)
}

// WriteSingle converts a single failed priority event into a dead-letter record and writes it.
func (w *Writer) WriteSingle(path string, batchID string, event model.IngestEvent, errClass string, errMsg string, retryCount int) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return w.WriteRecords([]Record{{
		Path:         path,
		BatchID:      batchID,
		Event:        event,
		ErrorClass:   errClass,
		ErrorMessage: errMsg,
		RetryCount:   retryCount,
		FirstFailedAt: now,
		LastFailedAt:  now,
	}})
}

// Stats returns the current file count and approximate total size.
func (w *Writer) Stats() (fileCount int, totalSize int64, oldestFileTime time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, 0, time.Time{}
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		fileCount++
		totalSize += info.Size()
		if oldestFileTime.IsZero() || info.ModTime().Before(oldestFileTime) {
			oldestFileTime = info.ModTime()
		}
	}
	return
}

// getOrOpenFile returns the current day's NDJSON file handle, rotating if needed.
func (w *Writer) getOrOpenFile() (*os.File, error) {
	today := time.Now().UTC().Format("2006-01-02")
	if w.file != nil && w.date == today {
		return w.file, nil
	}

	// Close previous day's file
	if w.file != nil {
		w.file.Close()
	}

	filename := filepath.Join(w.dir, fmt.Sprintf("dlq-%s.ndjson", today))
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open dead-letter file %s: %w", filename, err)
	}

	w.file = f
	w.date = today
	return f, nil
}

// Close flushes and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}