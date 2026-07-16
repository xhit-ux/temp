package stream

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"opc2ymatrix/model"
)

// Event represents a real-time message pushed to SSE clients.
// Includes persistence_status so frontends can distinguish buffered vs committed.
type Event struct {
	Type               string             `json:"type"`
	Event              model.IngestEvent  `json:"event"`
	PersistenceStatus  string             `json:"persistence_status"` // "buffered" or "committed"
}

// Broker manages SSE client connections and broadcasts events to all connected clients.
type Broker struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}  // each client has a message channel
}

// NewBroker creates a new SSE broker.
func NewBroker() *Broker {
	return &Broker{
		clients: make(map[chan []byte]struct{}),
	}
}

// Subscribe registers a new SSE client and returns its message channel.
// The caller is responsible for closing the channel when the client disconnects.
func (b *Broker) Subscribe() chan []byte {
	ch := make(chan []byte, 256)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	log.Printf("[SSE] Client connected, total clients: %d", len(b.clients))
	return ch
}

// Unsubscribe removes a client's message channel.
func (b *Broker) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	close(ch)
	b.mu.Unlock()
	log.Printf("[SSE] Client disconnected, total clients: %d", len(b.clients))
}

// Broadcast sends an event to all connected SSE clients.
// Non-blocking — slow clients are skipped.
func (b *Broker) Broadcast(event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[SSE] ERROR marshaling event: %v", err)
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.clients {
		select {
		case ch <- data:
		default:
			// Client buffer full, skip
		}
	}
}

// BroadcastIngest pushes an ingested event to all SSE clients with status "buffered".
func (b *Broker) BroadcastIngest(event model.IngestEvent) {
	b.Broadcast(Event{
		Type:              "point_update",
		Event:             event,
		PersistenceStatus: "buffered",
	})
}

// BroadcastCommitted is designed to be called after a batch is successfully written.
// For simplicity we broadcast per-event ingest.
func (b *Broker) BroadcastCommitted(event model.IngestEvent) {
	b.Broadcast(Event{
		Type:              "point_update",
		Event:             event,
		PersistenceStatus: "committed",
	})
}

// HandleSSE serves the SSE endpoint at /api/v1/stream.
func (b *Broker) HandleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	// Send initial connection event
	fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\"}\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: point_update\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}