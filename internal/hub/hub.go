// Package hub es un fan-out de mensajes a todos los clientes WebSocket conectados.
package hub

import "sync"

type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func New() *Hub {
	return &Hub{clients: make(map[chan []byte]struct{})}
}

// Register añade un cliente y devuelve su canal + una función para darse de baja.
func (h *Hub) Register() (chan []byte, func()) {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if _, ok := h.clients[ch]; ok {
			delete(h.clients, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
}

// Broadcast envía a todos los clientes. Si un cliente va lento (canal lleno), se salta el mensaje
// para no bloquear al resto (drop-oldest-style, sin backpressure global).
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
