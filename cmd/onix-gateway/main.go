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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"github.com/levapo97-cell/onix-gateway/internal/auth"
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
	authz := auth.New(os.Getenv("PANEL_USER"), os.Getenv("PANEL_PASSWORD"), os.Getenv("PANEL_PASSWORD_HASH"), os.Getenv("JWT_SECRET"))
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
	// ─── Auth (login del panel) ───
	loginLimiter := newRateLimiter(5, time.Minute) // 5 intentos/min por IP
	mux.HandleFunc("POST /auth/login", func(w http.ResponseWriter, r *http.Request) {
		if !loginLimiter.allow(clientIP(r)) {
			writeJSON(w, 429, map[string]any{"error": "demasiados intentos, espera un momento"})
			return
		}
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]any{"error": "body inválido"})
			return
		}
		tok, err := authz.Login(body.Username, body.Password)
		if err != nil {
			writeJSON(w, 401, map[string]any{"error": "credenciales inválidas"})
			return
		}
		writeJSON(w, 200, map[string]any{"token": tok, "user": body.Username})
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, []any{})
			return
		}
		q := r.URL.Query()
		f := store.EventFilters{AgentRole: q.Get("agent"), Type: q.Get("type"), Q: q.Get("q")}
		if s := q.Get("stage"); s != "" {
			if n, err := strconv.Atoi(s); err == nil {
				f.Stage = &n
			}
		}
		if l := q.Get("limit"); l != "" {
			f.Limit, _ = strconv.Atoi(l)
		}
		rows, err := st.EventsFiltered(r.Context(), f)
		if err != nil {
			slog.Error("/api/events falló", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, rows)
	})
	mux.HandleFunc("GET /api/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 404, map[string]any{"error": "no db"})
			return
		}
		d, err := st.EventDetail(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, 404, map[string]any{"error": "no existe"})
			return
		}
		writeJSON(w, 200, d)
	})
	mux.HandleFunc("GET /api/postmortem", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 200, map[string]any{"conclusion": "sin datos"})
			return
		}
		pm, err := st.PostMortem(r.Context())
		if err != nil {
			slog.Error("/api/postmortem", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, pm)
	})
	mux.HandleFunc("POST /api/export", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 503, map[string]any{"error": "no db"})
			return
		}
		var b struct {
			Scope  string `json:"scope"`
			Format string `json:"format"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		content, ctype, filename, err := st.ExportEvents(r.Context(), b.Scope, b.Format)
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": "export falló"})
			return
		}
		w.Header().Set("Content-Type", ctype)
		w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
		_, _ = w.Write([]byte(content))
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
	mux.HandleFunc("POST /api/tickets/{id}/attachment", func(w http.ResponseWriter, r *http.Request) {
		if st == nil {
			writeJSON(w, 503, map[string]any{"error": "no db"})
			return
		}
		id := r.PathValue("id")
		if !st.TicketExists(r.Context(), id) {
			writeJSON(w, 404, map[string]any{"error": "ticket no existe"})
			return
		}
		if err := r.ParseMultipartForm(8 << 20); err != nil { // máx 8 MB
			writeJSON(w, 400, map[string]any{"error": "archivo inválido o muy grande"})
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			writeJSON(w, 400, map[string]any{"error": "falta el campo 'file'"})
			return
		}
		defer file.Close()

		uploadDir := env("UPLOAD_DIR", "/data/uploads")
		_ = os.MkdirAll(uploadDir, 0o755)
		safeName := filepath.Base(header.Filename)
		dest := filepath.Join(uploadDir, id+"__"+safeName)
		out, err := os.Create(dest)
		if err != nil {
			slog.Error("guardar adjunto", "err", err)
			writeJSON(w, 500, map[string]any{"error": "no se pudo guardar"})
			return
		}
		if _, err := io.Copy(out, file); err != nil {
			out.Close()
			writeJSON(w, 500, map[string]any{"error": "no se pudo guardar"})
			return
		}
		out.Close()

		// Si es texto (plan/ticket), lee su contenido y añádelo al body para que el agente lo use.
		var text string
		if isTextFile(safeName) {
			if b, err := os.ReadFile(dest); err == nil && len(b) <= 262144 { // máx 256 KB de texto
				text = string(b)
			}
		}
		if err := st.SetAttachment(r.Context(), id, safeName, dest, text); err != nil {
			slog.Error("registrar adjunto", "err", err)
			writeJSON(w, 500, map[string]any{"error": "db"})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "filename": safeName, "ingested_text": text != ""})
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

	// ─── Proyectos / repos (Fase 6) — reenvía al onix-agent de la PC por NATS request-reply ───
	agentReq := func(w http.ResponseWriter, payload map[string]string, timeout time.Duration) {
		data, _ := json.Marshal(payload)
		msg, err := nc.Request("onix.agent.cmd", data, timeout)
		if err != nil {
			writeJSON(w, 502, map[string]any{"error": "onix-agent no disponible (¿está corriendo en tu PC?)"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(msg.Data)
	}
	mux.HandleFunc("GET /api/projects", func(w http.ResponseWriter, _ *http.Request) {
		agentReq(w, map[string]string{"action": "project.list"}, 8*time.Second)
	})
	mux.HandleFunc("POST /api/projects/check", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Name string `json:"name"` }
		_ = json.NewDecoder(r.Body).Decode(&b)
		agentReq(w, map[string]string{"action": "repo.check", "name": b.Name}, 8*time.Second)
	})
	mux.HandleFunc("POST /api/projects/clone", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ URL string `json:"url"` }
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.URL == "" {
			writeJSON(w, 400, map[string]any{"error": "url es obligatorio"})
			return
		}
		agentReq(w, map[string]string{"action": "repo.clone", "url": b.URL}, 5*time.Minute)
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
	mux.HandleFunc("/ws", wsHandler(h, authz))

	srv := &http.Server{Addr: ":" + port, Handler: withCORS(authGate(authz, mux)), ReadHeaderTimeout: 5 * time.Second}
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

func wsHandler(h *hub.Hub, a *auth.Auth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Auth del WS por query param (?token=<jwt>), ya que el navegador no manda headers en WS.
		if !a.ValidToken(r.URL.Query().Get("token")) {
			http.Error(w, "no autorizado", http.StatusUnauthorized)
			return
		}
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

// authGate exige JWT en las rutas /api/*. El resto (/, /healthz, /auth/login, /ws) pasa:
// /ws valida el token por query param dentro de wsHandler.
func authGate(a *auth.Auth, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodOptions && strings.HasPrefix(r.URL.Path, "/api/") {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !a.ValidToken(tok) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"no autorizado"}`))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
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

// rateLimiter: límite simple en memoria (N eventos por ventana, por clave/IP). Para /auth/login.
type rateLimiter struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	max    int
	window time.Duration
}

func newRateLimiter(max int, window time.Duration) *rateLimiter {
	return &rateLimiter{hits: make(map[string][]time.Time), max: max, window: window}
}

func (l *rateLimiter) allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-l.window)
	kept := l.hits[key][:0]
	for _, t := range l.hits[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= l.max {
		l.hits[key] = kept
		return false
	}
	l.hits[key] = append(kept, now)
	return true
}

func isTextFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".md", ".txt", ".json", ".yaml", ".yml", ".csv", ".log", ".sql":
		return true
	}
	return false
}

func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		if i := strings.IndexByte(xf, ','); i >= 0 {
			return strings.TrimSpace(xf[:i])
		}
		return strings.TrimSpace(xf)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	return host
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
