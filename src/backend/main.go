package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"opc2ymatrix/config"
	"opc2ymatrix/deadletter"
	"opc2ymatrix/handler"
	"opc2ymatrix/metrics"
	"opc2ymatrix/model"
	"opc2ymatrix/query"
	"opc2ymatrix/store"
	"opc2ymatrix/stream"
	"opc2ymatrix/writer"
)

func main() {
	envPath := flag.String("env", "", "Path to .env file (default: searches current dir and ../../.env)")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*envPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("[Main] Starting OPC2YMatrix Go Backend on port %d", cfg.ServerPort)
	log.Printf("[Main] YMatrix target: %s:%d/%s", cfg.DBHost, cfg.DBPort, cfg.DBDatabase)

	// Create root context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database connection pool
	db, err := store.NewDB(ctx, cfg.DSN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize metrics tracker
	m := metrics.NewTracker()
	m.StartPeriodicReport(10 * time.Second)

	// Initialize dead-letter writer
	dlq, err := deadletter.NewWriter(cfg.DeadLetterDir)
	if err != nil {
		log.Fatalf("Failed to initialize dead-letter writer: %v", err)
	}
	defer dlq.Close()
	log.Printf("[Main] Dead-letter directory: %s", cfg.DeadLetterDir)

	// Create dual in-memory event channels
	normalCh := make(chan model.IngestEvent, cfg.QueueCapacity)
	priorityCh := make(chan model.IngestEvent, cfg.PriorityQueueCapacity)
	log.Printf("[Main] Channels: normal=%d priority=%d", cfg.QueueCapacity, cfg.PriorityQueueCapacity)

	// Build batch writer configuration from env
	batchCfg := writer.BatchConfig{
		BatchSize:      cfg.BatchSize,
		FlushInterval:  time.Duration(cfg.FlushSeconds) * time.Second,
		MaxRetries:     cfg.MaxRetries,
		RetryBaseDelay: time.Duration(cfg.RetryBaseDelay) * time.Second,
		WriterPoolSize: cfg.WriterPoolSize,
	}

	var wg sync.WaitGroup

	// Acquire a dedicated connection for the priority writer (used by replayer too)
	prioConn, err := db.Pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire DB connection for priority writer: %v", err)
	}
	prioPgx := prioConn.Conn()

	// Start normal writer pool
	for i := 0; i < cfg.WriterPoolSize; i++ {
		dbConn, err := db.Pool.Acquire(ctx)
		if err != nil {
			log.Fatalf("Failed to acquire DB connection for normal writer %d: %v", i, err)
		}
		pgxConn := dbConn.Conn()

		bw := writer.NewBatchWriter(normalCh, pgxConn, m, dlq, batchCfg)

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer dbConn.Release()
			log.Printf("[Main] Normal writer %d started", idx)
			bw.Start(ctx)
			log.Printf("[Main] Normal writer %d stopped", idx)
		}(i)
	}

	// Start priority writer (single worker with dedicated connection)
	prioWriter := writer.NewPriorityWriter(priorityCh, prioPgx, m, dlq, batchCfg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer prioConn.Release()
		log.Println("[Main] Priority writer started")
		prioWriter.Start(ctx)
		log.Println("[Main] Priority writer stopped")
	}()

	log.Printf("[Main] Writer pool: %d normal + 1 priority", cfg.WriterPoolSize)

	// Initialize dead-letter replayer
	replayer, err := deadletter.NewReplayer(
		cfg.DeadLetterDir,
		time.Duration(cfg.DeadLetterReplayInterval)*time.Second,
		cfg.DeadLetterReplayBatchSize,
		m,
		func(ctx context.Context, batch []model.IngestEvent, batchID string) error {
			return prioWriter.InsertOnConflict(ctx, batch, batchID)
		},
	)
	if err != nil {
		log.Fatalf("Failed to initialize dead-letter replayer: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		replayer.Start(ctx)
	}()

	log.Printf("[Main] Dead-letter replayer started (interval=%ds, batch_size=%d)",
		cfg.DeadLetterReplayInterval, cfg.DeadLetterReplayBatchSize)

	// Initialize SSE broker
	sseBroker := stream.NewBroker()

	// Set up HTTP routes
	// CORS middleware — browser opens index.html from file:// so origin is null
	corsHandler := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	mux := http.NewServeMux()

	ingestHandler := handler.NewIngestHandler(normalCh, priorityCh, m, sseBroker)

	// Register query endpoints
	queryHandler := query.NewHandler(db.Pool)
	queryHandler.RegisterRoutes(mux)

	// Register SSE endpoint
	mux.HandleFunc("/api/v1/stream", sseBroker.HandleSSE)
	mux.HandleFunc("/api/v1/ingest/events", ingestHandler.Handle)
	mux.HandleFunc("/api/v1/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/api/v1/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not ready","error":"database ping failed"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := m.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp, _ := json.Marshal(map[string]interface{}{
			"total_received":            snap.TotalReceived,
			"total_written":             snap.TotalWritten,
			"total_failed":              snap.TotalFailed,
			"total_dropped":             snap.TotalDropped,
			"total_ambiguous_resolved":  snap.TotalAmbiguousResolved,
			"total_dead_letter":         snap.TotalDeadLetter,
			"total_dead_replayed":       snap.TotalDeadReplayed,
			"current_in_queue":          snap.CurrentInQueue,
			"batches_written":           snap.BatchesWritten,
			"batches_failed":            snap.BatchesFailed,
			"batches_active":            snap.BatchesActive,
			"rate_per_second":           snap.RatePerSecond,
			"avg_write_latency":         snap.AvgWriteLatency,
			"max_write_latency":         snap.MaxWriteLatency,
		})
		w.Write(resp)
	})
	// Admin endpoints
	mux.HandleFunc("/api/v1/admin/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"flush requested","note":"writers will flush on their next cycle"}`))
	})
	mux.HandleFunc("/api/v1/admin/dead-letter/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		replayed, failed, err := replayer.ReplayNow(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":    err.Error(),
				"replayed": replayed,
				"failed":   failed,
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"replayed": replayed,
			"failed":   failed,
		})
	})
	mux.HandleFunc("/api/v1/admin/dead-letter/stats", func(w http.ResponseWriter, r *http.Request) {
		fileCount, totalSize, oldestTime := dlq.Stats()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"file_count":       fileCount,
			"total_size_bytes": totalSize,
			"oldest_file_time": oldestTime.Format(time.RFC3339),
		})
	})

	server := &http.Server{
		Addr:         cfg.HTTPAddress(),
		Handler:      corsHandler(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // SSE long-lived connections must not have write timeout
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("[Main] HTTP server listening on %s", cfg.HTTPAddress())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[Main] Received signal %v, initiating graceful shutdown...", sig)

	// Cancel context to stop writers + replayer
	cancel()

	// Shutdown HTTP server with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Main] HTTP server forced to shutdown: %v", err)
	}

	// Stop replayer
	replayer.Stop()

	// Close channels to signal writers
	close(priorityCh)
	close(normalCh)

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("[Main] Graceful shutdown complete")
	m.LogReport()
}