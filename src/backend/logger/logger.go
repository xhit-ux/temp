package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Level represents log severity.
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func levelFromString(s string) Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

// Entry is a single log record.
type Entry struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Module    string    `json:"module"`
	Message   string    `json:"message"`
	Extra     string    `json:"extra,omitempty"`
}

// Logger is the central structured logger.
//
// Writes to YMatrix opc_operation_log table by default.  If the database is
// unavailable, falls back to local NDJSON files.  Both DB rows and file lines
// older than 24 hours are automatically cleaned up.
type Logger struct {
	db          *pgxpool.Pool
	fileDir     string
	currentFile *os.File
	fileMu      sync.Mutex

	minLevel    Level
	bufferSize  int
	buffer      chan Entry
	stopCh      chan struct{}
	doneCh      chan struct{}

	// Stats (approximate, for /api/v1/logs/stats)
	statsMu     sync.RWMutex
	totalByLevel map[Level]int64
}

// Config holds logger configuration.
type Config struct {
	DB         *pgxpool.Pool // optional — if nil, logs file-only
	FileDir    string        // fallback file directory
	MinLevel   Level         // minimum level to record
	BufferSize int           // async buffer size
	Retention  time.Duration // retention window (default 24h)
}

// New creates and starts an async Logger.
func New(cfg Config) (*Logger, error) {
	if cfg.FileDir == "" {
		cfg.FileDir = "data/logs"
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 2000
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 24 * time.Hour
	}

	if err := os.MkdirAll(cfg.FileDir, 0755); err != nil {
		return nil, fmt.Errorf("create log file directory %s: %w", cfg.FileDir, err)
	}

	l := &Logger{
		db:           cfg.DB,
		fileDir:      cfg.FileDir,
		minLevel:     cfg.MinLevel,
		bufferSize:   cfg.BufferSize,
		buffer:       make(chan Entry, cfg.BufferSize),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		totalByLevel: make(map[Level]int64),
	}

	// Open the current hour's file for fallback
	if err := l.rotateFile(); err != nil {
		log.Printf("[Logger] WARNING: could not open fallback log file: %v", err)
	}

	go l.worker(cfg.Retention)

	log.Printf("[Logger] Started (min_level=%s, buffer=%d, retention=%s)", l.minLevel, l.bufferSize, cfg.Retention)
	return l, nil
}

// Log enqueues a structured log entry.  Non-blocking: if the buffer is full,
// the entry is dropped and a warning is printed to stderr.
func (l *Logger) Log(level Level, module, message string, extra ...string) {
	if level < l.minLevel {
		return
	}

	entry := Entry{
		ID:        uuid.New().String(),
		Timestamp: time.Now().UTC(),
		Level:     level.String(),
		Module:    module,
		Message:   message,
	}
	if len(extra) > 0 {
		entry.Extra = extra[0]
	}

	select {
	case l.buffer <- entry:
	default:
		// Buffer full — drop oldest and insert new
		select {
		case <-l.buffer:
		default:
		}
		// Try again after draining one
		select {
		case l.buffer <- entry:
		default:
			fmt.Fprintf(os.Stderr, "[Logger] DROPPED: buffer full, entry lost (module=%s level=%s)\n", module, level)
		}
	}
}

// Debug logs at DEBUG level.
func (l *Logger) Debug(module, message string, extra ...string) {
	l.Log(DEBUG, module, message, extra...)
}

// Info logs at INFO level.
func (l *Logger) Info(module, message string, extra ...string) {
	l.Log(INFO, module, message, extra...)
}

// Warn logs at WARN level.
func (l *Logger) Warn(module, message string, extra ...string) {
	l.Log(WARN, module, message, extra...)
}

// Error logs at ERROR level.
func (l *Logger) Error(module, message string, extra ...string) {
	l.Log(ERROR, module, message, extra...)
}

// Stop flushes and shuts down the logger.
func (l *Logger) Stop() {
	close(l.stopCh)
	<-l.doneCh

	// Drain remaining buffer
	close(l.buffer)
	for entry := range l.buffer {
		l.writeEntry(entry)
	}

	if l.currentFile != nil {
		l.currentFile.Close()
		l.currentFile = nil
	}

	log.Println("[Logger] Stopped")
}

// ctx returns a background context for async DB writes.
// Uses a short timeout to avoid blocking the worker indefinitely.
func (l *Logger) bgCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (l *Logger) worker(retention time.Duration) {
	defer close(l.doneCh)

	// Cleanup ticker: every 10 minutes
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	// File rotation ticker: every hour
	rotateTicker := time.NewTicker(1 * time.Hour)
	defer rotateTicker.Stop()

	// Run initial cleanup
	l.cleanup(retention)

	for {
		select {
		case <-l.stopCh:
			return

		case entry := <-l.buffer:
			l.writeEntry(entry)

		case <-cleanupTicker.C:
			l.cleanup(retention)

		case <-rotateTicker.C:
			if err := l.rotateFile(); err != nil {
				fmt.Fprintf(os.Stderr, "[Logger] file rotation failed: %v\n", err)
			}
		}
	}
}

func (l *Logger) writeEntry(entry Entry) {
	// Update stats
	l.statsMu.Lock()
	l.totalByLevel[levelFromString(entry.Level)]++
	l.statsMu.Unlock()

	// Try DB first
	if l.db != nil {
		ctx, cancel := l.bgCtx()
		err := l.writeToDB(ctx, entry)
		cancel()
		if err == nil {
			return
		}
		// DB write failed — fall through to file
		fmt.Fprintf(os.Stderr, "[Logger] DB write failed (falling back to file): %v\n", err)
	}

	// File fallback
	l.writeToFile(entry)
}

func (l *Logger) writeToDB(ctx context.Context, entry Entry) error {
	_, err := l.db.Exec(ctx,
		`INSERT INTO opc_operation_log (id, timestamp, level, module, message, extra)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		entry.ID, entry.Timestamp, entry.Level, entry.Module, entry.Message, entry.Extra,
	)
	return err
}

func (l *Logger) writeToFile(entry Entry) {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()

	if l.currentFile == nil {
		if err := l.rotateFile(); err != nil {
			fmt.Fprintf(os.Stderr, "[Logger] cannot write to file: %v\n", err)
			return
		}
	}

	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Logger] JSON marshal failed: %v\n", err)
		return
	}
	data = append(data, '\n')

	if _, err := l.currentFile.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "[Logger] file write failed: %v\n", err)
	}
}

// rotateFile closes the current file and opens a new one named by the current hour.
func (l *Logger) rotateFile() error {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()

	if l.currentFile != nil {
		l.currentFile.Close()
		l.currentFile = nil
	}

	name := fmt.Sprintf("ops-%s.ndjson", time.Now().UTC().Format("2006-01-02T15"))
	path := filepath.Join(l.fileDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", path, err)
	}
	l.currentFile = f
	return nil
}

// cleanup removes DB rows and files older than the retention window.
func (l *Logger) cleanup(retention time.Duration) {
	cutoff := time.Now().UTC().Add(-retention)

	// DB cleanup
	if l.db != nil {
		ctx, cancel := l.bgCtx()
		_, err := l.db.Exec(ctx, `DELETE FROM opc_operation_log WHERE timestamp < $1`, cutoff)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Logger] DB cleanup failed: %v\n", err)
		}
	}

	// File cleanup: remove files whose name encodes a time before cutoff
	entries, err := os.ReadDir(l.fileDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		// Parse file name: ops-2006-01-02T15.ndjson
		base := strings.TrimSuffix(e.Name(), ".ndjson")
		base = strings.TrimPrefix(base, "ops-")
		t, err := time.Parse("2006-01-02T15", base)
		if err != nil {
			continue
		}
		// Keep files whose hour is at or after the cutoff hour
		if t.Before(cutoff.Truncate(time.Hour)) {
			path := filepath.Join(l.fileDir, e.Name())
			os.Remove(path)
		}
	}
}

// ------------------- Query helpers -------------------

// LogFilter specifies criteria for querying logs.
type LogFilter struct {
	Level   string // empty = all
	Module  string // empty = all
	Keyword string // empty = no keyword filter
	From    string // RFC3339 or empty
	To      string // RFC3339 or empty
	Limit   int    // 0 = default 200
	Offset  int
}

// QueryResult holds the result of a log query.
type QueryResult struct {
	Entries []Entry `json:"entries"`
	Total   int     `json:"total"`
	HasMore bool    `json:"has_more"`
}

// Query searches logs from DB. Falls back to file scan if DB is unavailable.
func (l *Logger) Query(ctx context.Context, filter LogFilter) (*QueryResult, error) {
	if filter.Limit <= 0 {
		filter.Limit = 200
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000
	}

	if l.db != nil {
		return l.queryDB(ctx, filter)
	}
	return l.queryFiles(filter)
}

func (l *Logger) queryDB(ctx context.Context, filter LogFilter) (*QueryResult, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	argIdx := 1

	if filter.Level != "" {
		where = append(where, fmt.Sprintf("level = $%d", argIdx))
		args = append(args, strings.ToUpper(filter.Level))
		argIdx++
	}
	if filter.Module != "" {
		where = append(where, fmt.Sprintf("module ILIKE $%d", argIdx))
		args = append(args, "%"+filter.Module+"%")
		argIdx++
	}
	if filter.Keyword != "" {
		where = append(where, fmt.Sprintf("message ILIKE $%d", argIdx))
		args = append(args, "%"+filter.Keyword+"%")
		argIdx++
	}
	if filter.From != "" {
		t, err := time.Parse(time.RFC3339, filter.From)
		if err == nil {
			where = append(where, fmt.Sprintf("timestamp >= $%d", argIdx))
			args = append(args, t)
			argIdx++
		}
	}
	if filter.To != "" {
		t, err := time.Parse(time.RFC3339, filter.To)
		if err == nil {
			where = append(where, fmt.Sprintf("timestamp <= $%d", argIdx))
			args = append(args, t)
			argIdx++
		}
	}

	whereClause := strings.Join(where, " AND ")

	// Count total
	var total int
	countQuery := fmt.Sprintf("SELECT count(*) FROM opc_operation_log WHERE %s", whereClause)
	if err := l.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count logs: %w", err)
	}

	// Query entries
	dataQuery := fmt.Sprintf(
		"SELECT id, timestamp, level, module, message, COALESCE(extra,'') FROM opc_operation_log WHERE %s ORDER BY timestamp DESC LIMIT $%d OFFSET $%d",
		whereClause, argIdx, argIdx+1,
	)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := l.db.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.Level, &e.Module, &e.Message, &e.Extra); err != nil {
			return nil, fmt.Errorf("scan log row: %w", err)
		}
		// Level is already a string from DB
		entries = append(entries, e)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if entries == nil {
		entries = []Entry{}
	}

	return &QueryResult{
		Entries: entries,
		Total:   total,
		HasMore: filter.Offset+filter.Limit < total,
	}, nil
}

// queryFiles scans NDJSON log files and returns matching entries.
// This is a fallback when DB is unavailable.
func (l *Logger) queryFiles(filter LogFilter) (*QueryResult, error) {
	entries, err := os.ReadDir(l.fileDir)
	if err != nil {
		return nil, fmt.Errorf("read log dir: %w", err)
	}

	// Sort files newest first
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() > entries[j].Name()
	})

	var allEntries []Entry
	levelFilter := strings.ToUpper(filter.Level)
	keywordLower := strings.ToLower(filter.Keyword)
	moduleLower := strings.ToLower(filter.Module)

	var fromTime, toTime time.Time
	if filter.From != "" {
		fromTime, _ = time.Parse(time.RFC3339, filter.From)
	}
	if filter.To != "" {
		toTime, _ = time.Parse(time.RFC3339, filter.To)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ndjson" {
			continue
		}
		path := filepath.Join(l.fileDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			var e Entry
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				continue
			}

			// Apply filters
			if levelFilter != "" && strings.ToUpper(e.Level) != levelFilter {
				continue
			}
			if moduleLower != "" && !strings.Contains(strings.ToLower(e.Module), moduleLower) {
				continue
			}
			if keywordLower != "" && !strings.Contains(strings.ToLower(e.Message), keywordLower) {
				continue
			}
			if !fromTime.IsZero() && e.Timestamp.Before(fromTime) {
				continue
			}
			if !toTime.IsZero() && e.Timestamp.After(toTime) {
				continue
			}

			allEntries = append(allEntries, e)
		}
	}

	// Sort newest first
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Timestamp.After(allEntries[j].Timestamp)
	})

	total := len(allEntries)
	start := filter.Offset
	if start > total {
		start = total
	}
	end := start + filter.Limit
	if end > total {
		end = total
	}

	entries2 := allEntries[start:end]
	if entries2 == nil {
		entries2 = []Entry{}
	}

	return &QueryResult{
		Entries: entries2,
		Total:   total,
		HasMore: end < total,
	}, nil
}

// LogStats returns counts by log level.
type LogStats struct {
	Total    int64            `json:"total"`
	ByLevel  map[string]int64 `json:"by_level"`
	FileMode bool             `json:"file_mode"` // true if DB is unavailable
}

// Stats returns approximate log counts by level.
func (l *Logger) Stats() LogStats {
	l.statsMu.RLock()
	defer l.statsMu.RUnlock()

	byLevel := make(map[string]int64)
	var total int64
	for level, count := range l.totalByLevel {
		byLevel[level.String()] = count
		total += count
	}
	return LogStats{
		Total:    total,
		ByLevel:  byLevel,
		FileMode: l.db == nil,
	}
}
// NewLogWriter returns an io.Writer that bridges Go's standard log output
// into the structured Logger.  Each line becomes a structured log entry
// with module extracted from the [Module] prefix pattern.
type logWriter struct {
	logger *Logger
}

func NewLogWriter(l *Logger) io.Writer {
	return &logWriter{logger: l}
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))

	// Extract module from "[Module] message" pattern
	module := "Go"
	message := msg
	if idx := strings.Index(msg, "] "); idx > 0 && strings.HasPrefix(msg, "[") {
		module = msg[1:idx]
		message = msg[idx+2:]
	}

	// Detect level from keywords
	level := INFO
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "error") || strings.Contains(lower, "fatal") || strings.Contains(lower, "critical") {
		level = ERROR
	} else if strings.Contains(lower, "warning") || strings.Contains(lower, "warn") {
		level = WARN
	} else if strings.Contains(lower, "debug") {
		level = DEBUG
	}

	w.logger.Log(level, module, message)
	return len(p), nil
}