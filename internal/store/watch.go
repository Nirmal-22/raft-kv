package store

import (
	"strings"
	"sync"
)

// EventType says what happened to a key.
type EventType string

const (
	EventPut    EventType = "put"
	EventDelete EventType = "delete"
	EventExpire EventType = "expire"
)

// Event is one change to one key, published after the change is committed
// locally.
type Event struct {
	Type    EventType `json:"type"`
	Key     string    `json:"key"`
	Value   []byte    `json:"-"`
	Version uint64    `json:"version"`
}

const watchBuffer = 256

type subscriber struct {
	prefix string
	ch     chan Event
}

// Hub fans changes out to watchers. It never blocks the apply loop: a watcher
// that falls more than watchBuffer events behind is cut off (its channel is
// closed) and has to reconnect. Watchers are also cut off when the store is
// replaced by a snapshot, because those changes produce no events.
type Hub struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newHub() *Hub { return &Hub{subs: make(map[*subscriber]struct{})} }

func (h *Hub) subscribe(prefix string) (<-chan Event, func()) {
	sub := &subscriber{prefix: prefix, ch: make(chan Event, watchBuffer)}
	h.mu.Lock()
	h.subs[sub] = struct{}{}
	h.mu.Unlock()
	cancel := func() {
		h.mu.Lock()
		if _, ok := h.subs[sub]; ok {
			delete(h.subs, sub)
			close(sub.ch)
		}
		h.mu.Unlock()
	}
	return sub.ch, cancel
}

func (h *Hub) publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs {
		if !strings.HasPrefix(ev.Key, sub.prefix) {
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			delete(h.subs, sub)
			close(sub.ch)
		}
	}
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs {
		delete(h.subs, sub)
		close(sub.ch)
	}
}
