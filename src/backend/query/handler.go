package query

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler serves the four query endpoints defined in V2.0 Section 14.
type Handler struct {
	pool *pgxpool.Pool
}

func NewHandler(pool *pgxpool.Pool) *Handler {
	return &Handler{pool: pool}
}

type TrendPoint struct {
	SourceTimestamp time.Time `json:"source_timestamp"`
	DeviceID        string    `json:"device_id"`
	PointName       string    `json:"point_name"`
	ValueNumber     *float64  `json:"value_number"`
	QualityName     string    `json:"quality_name"`
}

type AbnormalPoint struct {
	EventID        string    `json:"event_id"`
	DeviceID       string    `json:"device_id"`
	PointName      string    `json:"point_name"`
	ValueNumber    *float64  `json:"value_number"`
	ValueText      *string   `json:"value_text"`
	ValueTime      *string   `json:"value_time"`
	QualityName    string    `json:"quality_name"`
	AbnormalReason string    `json:"abnormal_reason"`
	SourceTimestamp time.Time `json:"source_timestamp"`
}

type DeviceStats struct {
	DeviceID       string    `json:"device_id"`
	SampleCount    int64     `json:"sample_count"`
	AbnormalCount  int64     `json:"abnormal_count"`
	FirstSampleTime time.Time `json:"first_sample_time"`
	LastSampleTime  time.Time `json:"last_sample_time"`
}

type Alarm struct {
	AlarmID      string     `json:"alarm_id"`
	DeviceID     string     `json:"device_id"`
	PointName    *string    `json:"point_name"`
	Severity     string     `json:"severity"`
	AlarmType    string     `json:"alarm_type"`
	Message      string     `json:"message"`
	Status       string     `json:"status"`
	OccurredAt   time.Time  `json:"occurred_at"`
	RecoveredAt  *time.Time `json:"recovered_at"`
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/trends", h.handleTrends)
	mux.HandleFunc("/api/v1/abnormal-points", h.handleAbnormalPoints)
	mux.HandleFunc("/api/v1/device-statistics", h.handleDeviceStatistics)
	mux.HandleFunc("/api/v1/alarms", h.handleAlarms)
	mux.HandleFunc("/api/v1/admin/sql", h.handleSQLConsole)
}

func (h *Handler) handleSQLConsole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Query == "" {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := h.pool.Query(ctx, body.Query)
	if err != nil {
		w.Header().Set("Content-Type","application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
		return
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = string(col.Name)
	}
	var results []map[string]interface{}
	for rows.Next() {
		vals, _ := rows.Values()
		row := make(map[string]interface{})
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		results = append(results, row)
	}
	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type","application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"columns": colNames,
		"rows": results,
		"row_count": len(results),
		"latency_ms": elapsed,
	})
}

func (h *Handler) handleTrends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	deviceID := r.URL.Query().Get("device_id")
	pointName := r.URL.Query().Get("point_name")
	if deviceID == "" || pointName == "" {
		http.Error(w, `{"error":"device_id and point_name are required"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := h.pool.Query(ctx,
		`SELECT source_timestamp, device_id, point_name, value_number, quality_name
		 FROM opc_point_data WHERE device_id=$1 AND point_name=$2
		 AND source_timestamp >= now() - interval '5 minutes' ORDER BY source_timestamp`,
		deviceID, pointName)
	if err != nil {
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []TrendPoint
	for rows.Next() {
		var p TrendPoint
		if err := rows.Scan(&p.SourceTimestamp, &p.DeviceID, &p.PointName, &p.ValueNumber, &p.QualityName); err != nil {
			continue
		}
		points = append(points, p)
	}
	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Query-Latency-Ms", strconv.FormatInt(elapsed, 10))
	json.NewEncoder(w).Encode(map[string]interface{}{"points": points, "latency_ms": elapsed})
}

func (h *Handler) handleAbnormalPoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := h.pool.Query(ctx,
		`SELECT event_id,device_id,point_name,value_number,value_text,value_time,
		        quality_name,abnormal_reason,source_timestamp
		 FROM opc_point_data WHERE is_abnormal=true
		 AND source_timestamp >= now() - interval '24 hours'
		 ORDER BY source_timestamp DESC LIMIT $1`, limit)
	if err != nil {
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []AbnormalPoint
	for rows.Next() {
		var p AbnormalPoint
		var vt time.Time
		if err := rows.Scan(&p.EventID, &p.DeviceID, &p.PointName, &p.ValueNumber, &p.ValueText,
			&vt, &p.QualityName, &p.AbnormalReason, &p.SourceTimestamp); err != nil {
			log.Printf("[Query] Abnormal scan error: %v", err)
			continue
		}
		if !vt.IsZero() {
			s := vt.Format(time.RFC3339)
			p.ValueTime = &s
		}
		points = append(points, p)
	}
	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Query-Latency-Ms", strconv.FormatInt(elapsed, 10))
	json.NewEncoder(w).Encode(map[string]interface{}{"points": points, "latency_ms": elapsed})
}

func (h *Handler) handleDeviceStatistics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	now := time.Now().UTC()
	from := now.Add(-1 * time.Hour)
	to := now
	if fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = t
		}
	}
	if toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = t
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := h.pool.Query(ctx,
		`SELECT device_id,count(*) AS sample_count,
		        count(*) FILTER (WHERE is_abnormal) AS abnormal_count,
		        min(source_timestamp),max(source_timestamp)
		 FROM opc_point_data WHERE source_timestamp>=$1 AND source_timestamp<$2
		 GROUP BY device_id ORDER BY device_id`, from, to)
	if err != nil {
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var stats []DeviceStats
	for rows.Next() {
		var s DeviceStats
		if err := rows.Scan(&s.DeviceID, &s.SampleCount, &s.AbnormalCount, &s.FirstSampleTime, &s.LastSampleTime); err != nil {
			continue
		}
		stats = append(stats, s)
	}
	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Query-Latency-Ms", strconv.FormatInt(elapsed, 10))
	json.NewEncoder(w).Encode(map[string]interface{}{"stats": stats, "latency_ms": elapsed})
}

func (h *Handler) handleAlarms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := h.pool.Query(ctx,
		`SELECT alarm_id,device_id,point_name,severity,alarm_type,
		        message,status,occurred_at,recovered_at
		 FROM opc_alarm_event WHERE status='active'
		 ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
		 occurred_at DESC`)
	if err != nil {
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var alarms []Alarm
	for rows.Next() {
		var a Alarm
		if err := rows.Scan(&a.AlarmID, &a.DeviceID, &a.PointName, &a.Severity, &a.AlarmType,
			&a.Message, &a.Status, &a.OccurredAt, &a.RecoveredAt); err != nil {
			continue
		}
		alarms = append(alarms, a)
	}
	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Query-Latency-Ms", strconv.FormatInt(elapsed, 10))
	json.NewEncoder(w).Encode(map[string]interface{}{"alarms": alarms, "latency_ms": elapsed})
}