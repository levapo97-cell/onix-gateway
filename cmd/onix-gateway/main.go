// Command onix-gateway — edge público que consume el frontend OnixGuard.
//
// Se suscribe a onix.clean.* (eventos) y onix.report.* (reportes) en NATS y los empuja por
// WebSocket (/ws). Expone GET /api/events, y (Fase 3) GET /api/reports, GET /api/reports/{id}
// y POST /api/reports/{id}/respond (reenvía al orchestrator). /healthz. (Auth JWT: fase posterior.)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"github.com/levapo97-cell/onix-gateway/internal/hub"
	"github.com/levapo97-cell/onix-gateway/internal/store"
)

var version = "dev"

func main() {
	// -healthcheck: para el HEALTHCHECK de Docker (imagen distroless, sin shell/curl).
	hc := flag.Bool("healthcheck", false, "hace ping a /healthz y termina")
	flag.Parse()
	if *hc {
		os.Exit(runHealthcheck(env("PORT", "8080")))
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	port := env("PORT", "8080")
	natsURL := env("NATS_URL", "nats://nats:4222")
	databaseURL := os.Getenv("DATABASE_URL")
	orchestratorURL := env("ORCHESTRATOR_URL", "http://orchestrator:8085")
	slog.Info("onix-gateway arrancando", "version", version, "port", port, "nats_url", natsURL, "has_db", databaseURL != "")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	h := hub.New()

	// Postgres (para /api/events). Opcional: sin DB, /api/events devuelve [].
	var st *store.Store
	if databaseURL != "" {
		var err error
		st, err = store.New(ctx, databaseURL)
		if err != nil {
			slog.Error("no se pudo abrir Postgres", "err", err)
			os.Exit(1)
		}
		defer st.Close()
	}

	// NATS: suscripción viva (plain core; la durabilidad la maneja onix-core). Fan-out al hub.
	nc, err := nats.Connect(natsURL, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.Name("onix-gateway"))
	if err != nil {
		slog.Error("no se pudo conectar a NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()
	// FASE 2: el gateway empuja el evento LIMPIO (redactado y enriquecido), no el raw.
	sub, err := nc.Subscribe("onix.clean.>", func(m *nats.Msg) {
		// Reempaqueta el evento como WsMessage {type:"event", data:<evento>}.
		var data json.RawMessage = m.Data
		msg, _ := json.Marshal(map[string]any{"type": "event", "data": data})
		h.Broadcast(msg)
		// Señal de refresco de métricas (el panel vuelve a pedir /api/overview y /api/agents).
		h.Broadcast([]byte(`{"type":"metrics"}`))
	})
	if err != nil {
		slog.Error("no se pudo suscribir a NATS", "err", err)
		os.Exit(1)
	}
	defer sub.Drain()

	// FASE 3: el orchestrator avisa de reportes nuevos → empuja {type:"report"} al panel.
	subR, err := nc.Subscribe("onix.report.>", func(m *nats.Msg) {
		var data json.RawMessage = m.Data
		msg, _ := json.Marshal(map[string]any{"type": "report", "data": data})
		h.Broadcast(msg)
		// Un reporte (o su aprobación) puede cambiar la etapa y las métricas → refresca.
		h.Broadcast([]byte(`{"type":"stage"}`))
		h.Broadcast([]byte(`{"type":"metrics"}`))
	})
	if err == nil {
		defer subR.Drain()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "service": "onix-gateway", "version": version, "ws_clients": h.Count()})
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		rows, err := st.RecentEvents(r.Context(), 50)
		if err != nil {
			slog.Error("/api/events falló", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	// ─── Reportes (Fase 3) ───
	mux.HandleFunc("GET /api/reports", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		rows, err := st.Reports(r.Context(), 100)
		if err != nil {
			slog.Error("/api/reports falló", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	mux.HandleFunc("GET /api/reports/{id}", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 404, map[string]any{"error": "no db"})
			return
		}
		rep, err := st.Report(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, 404, map[string]any{"error": "no existe"})
			return
		}
		writeJSON(w, 200, rep)
	})
	mux.HandleFunc("POST /api/reports/{id}/respond", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Action  string `json:"action"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Action == "" {
			writeJSON(w, 400, map[string]any{"error": "action es obligatorio"})
			return
		}
		// Reenvía al orchestrator, que resuelve la llamada MCP pendiente del agente.
		payload, _ := json.Marshal(map[string]string{"report_id": r.PathValue("id"), "action": body.Action, "message": body.Message})
		resp, err := http.Post(orchestratorURL+"/respond", "application/json", bytes.NewReader(payload))
		if err != nil {
			slog.Error("no se pudo contactar al orchestrator", "err", err)
			writeJSON(w, 502, map[string]any{"error": "orchestrator no disponible"})
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
	// ─── Tickets (Fase 5) ───
	mux.HandleFunc("GET /api/tickets", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		rows, err := st.Tickets(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	mux.HandleFunc("POST /api/tickets", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 503, map[string]any{"error": "no db"})
			return
		}
		var body struct {
			Title  string `json:"title"`
			Body   string `json:"body"`
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Title == "" {
			writeJSON(w, 400, map[string]any{"error": "title es obligatorio"})
			return
		}
		id, err := st.CreateTicket(r.Context(), body.Title, body.Body, body.Source)
		if err != nil {
			slog.Error("crear ticket", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 201, map[string]any{"id": id})
	})
	mux.HandleFunc("GET /api/tickets/{id}", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 404, map[string]any{"error": "no db"})
			return
		}
		t, err := st.TicketDetail(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, 404, map[string]any{"error": "no existe"})
			return
		}
		writeJSON(w, 200, t)
	})

	// ─── Métricas de Monitoreo (Fase 4) ───
	mux.HandleFunc("GET /api/overview", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, store.Overview{TotalStages: 12, CurrentStage: 1})
			return
		}
		o, err := st.Overview(r.Context())
		if err != nil {
			slog.Error("/api/overview", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, o)
	})
	mux.HandleFunc("GET /api/stages", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		rows, err := st.StageList(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	mux.HandleFunc("GET /api/agents", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		rows, err := st.AgentList(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	mux.HandleFunc("/ws", wsHandler(h))

	srv := &http.Server{Addr: ":" + port, Handler: withCORS(mux), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	slog.Info("gateway escuchando", "addr", srv.Addr)

	select {
	case err := <-errCh:
		slog.Error("servidor falló", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		slog.Info("apagando…")
		sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}
}

// upgrader de WebSocket. En dev aceptamos cualquier origin; en prod se restringe.
var upgrader = websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}

func wsHandler(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		ch, unregister := h.Register()
		defer unregister()
		slog.Info("ws conectado", "clients", h.Count())

		// Lector: descarta mensajes del cliente pero detecta cierre/pong.
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		// Escritor: reenvía broadcasts + ping periódico de keepalive.
		ping := time.NewTicker(30 * time.Second)
		defer ping.Stop()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					return
				}
			case <-ping.C:
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runHealthcheck(port string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
