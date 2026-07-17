package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"opc2ymatrix/metrics"
	"opc2ymatrix/model"
	"opc2ymatrix/stream"
)

// IngestHandler receives events from the Python collector and routes them
// into either the normal (batch) channel or the priority (immediate) channel.
type IngestHandler struct {
	normalCh   chan<- model.IngestEvent
	priorityCh chan<- model.IngestEvent
	metrics    *metrics.Tracker
	broker     *stream.Broker
}

// NewIngestHandler creates a new ingest HTTP handler with dual-channel routing.
func NewIngestHandler(normalCh, priorityCh chan<- model.IngestEvent, m *metrics.Tracker, b *stream.Broker) *IngestHandler {
	return &IngestHandler{
		normalCh:   normalCh,
		priorityCh: priorityCh,
		metrics:    m,
		broker:     b,
	}
}

// Handle processes POST /api/v1/ingest/events.
func (h *IngestHandler) Handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var events model.IngestRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&events); err != nil {
		log.Printf("[Ingest] ERROR parsing request: %v", err)
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	if len(events) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	log.Printf("[Ingest] Received %d events", len(events))

	accepted := 0
	dropped := 0

	for i := range events {
		if err := events[i].Validate(); err != nil {
			log.Printf("[Ingest] WARNING event validation error (event_id=%s): %v", events[i].EventID, err)
			dropped++
			continue
		}

		var targetCh chan<- model.IngestEvent
		if events[i].IsAbnormal() {
			targetCh = h.priorityCh
		} else {
			targetCh = h.normalCh
		}

		select {
		case targetCh <- events[i]:
			accepted++
			if h.broker != nil {
				h.broker.BroadcastIngest(events[i])
			}
		default:
			dropped++
			log.Printf("[Ingest] WARNING channel full (%s), dropping event_id=%s device=%s/%s",
				channelLabel(targetCh == h.priorityCh),
				events[i].EventID, events[i].DeviceID, events[i].PointName)
		}
	}

	h.metrics.RecordReceived(int64(accepted))
	if dropped > 0 {
		h.metrics.RecordDropped(int64(dropped))
	}

	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if accepted == 0 && dropped > 0 {
		status = http.StatusTooManyRequests
	} else if dropped > 0 {
		// Partial success — signal to collector that some events were lost
		status = 207 // Multi-Status
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"accepted": accepted,
		"dropped":  dropped,
	})
}

func channelLabel(isPriority bool) string {
	if isPriority {
		return "priority"
	}
	return "normal"
}