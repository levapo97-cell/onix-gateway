package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func jsonLine(e LogEvent) (string, error) {
	b, err := json.Marshal(e)
	return string(b), err
}

// LogEvent: fila de la tabla de Logs (con filtros).
type LogEvent struct {
	ID           string    `json:"id"`
	Ts           time.Time `json:"ts"`
	Type         string    `json:"type"`
	Tool         *string   `json:"tool"`
	Summary      *string   `json:"summary"`
	AgentRole    string    `json:"agent_role"`
	Stage        *int      `json:"stage"`
	IsError      bool      `json:"is_error"`
	IsRepetition bool      `json:"is_repetition"`
}

// EventFilters: filtros de la pantalla Logs.
type EventFilters struct {
	AgentRole string
	Type      string
	Stage     *int
	Q         string
	Limit     int
}

func (s *Store) EventsFiltered(ctx context.Context, f EventFilters) ([]LogEvent, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 200
	}
	var where []string
	var args []any
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.AgentRole != "" {
		add("a.role = $%d", f.AgentRole)
	}
	if f.Type != "" {
		add("e.type = $%d", f.Type)
	}
	if f.Stage != nil {
		add("st.number = $%d", *f.Stage)
	}
	if f.Q != "" {
		args = append(args, f.Q)
		idx := len(args)
		where = append(where, fmt.Sprintf("(e.summary ILIKE '%%'||$%d||'%%' OR e.tool ILIKE '%%'||$%d||'%%')", idx, idx))
	}
	clause := ""
	if len(where) > 0 {
		clause = "WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit)
	q := fmt.Sprintf(`
		SELECT e.id, e.ts, e.type, e.tool, e.summary, a.role, st.number, e.is_error, e.is_repetition
		FROM events e
		JOIN agents a ON a.id = e.agent_id
		LEFT JOIN stages st ON st.id = e.stage_id
		%s
		ORDER BY e.ts DESC
		LIMIT $%d`, clause, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LogEvent{}
	for rows.Next() {
		var e LogEvent
		if err := rows.Scan(&e.ID, &e.Ts, &e.Type, &e.Tool, &e.Summary, &e.AgentRole, &e.Stage, &e.IsError, &e.IsRepetition); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Credential: lo que se muestra en el detalle (SOLO hash).
type Credential struct {
	Kind   string `json:"kind"`
	Sha256 string `json:"sha256"`
	Label  string `json:"label"`
}

// EventDetail: un evento + su tool_call y credenciales detectadas (hash).
type EventDetail struct {
	LogEvent
	ParamsNormalized *string      `json:"params_normalized"`
	ParamsHash       *string      `json:"params_hash"`
	ExitCode         *int         `json:"exit_code"`
	DurationMs       *int         `json:"duration_ms"`
	Credentials      []Credential `json:"credentials"`
}

func (s *Store) EventDetail(ctx context.Context, id string) (*EventDetail, error) {
	var d EventDetail
	if err := s.pool.QueryRow(ctx, `
		SELECT e.id, e.ts, e.type, e.tool, e.summary, a.role, st.number, e.is_error, e.is_repetition
		FROM events e JOIN agents a ON a.id=e.agent_id LEFT JOIN stages st ON st.id=e.stage_id
		WHERE e.id=$1`, id).Scan(
		&d.ID, &d.Ts, &d.Type, &d.Tool, &d.Summary, &d.AgentRole, &d.Stage, &d.IsError, &d.IsRepetition); err != nil {
		return nil, err
	}
	// tool_call (si hay)
	var pn *string
	_ = s.pool.QueryRow(ctx, `SELECT params_normalized::text, params_hash, exit_code, duration_ms FROM tool_calls WHERE event_id=$1 LIMIT 1`, id).
		Scan(&pn, &d.ParamsHash, &d.ExitCode, &d.DurationMs)
	d.ParamsNormalized = pn
	// credenciales (solo hash)
	d.Credentials = []Credential{}
	rows, err := s.pool.Query(ctx, `SELECT kind, sha256, label FROM credentials_detected WHERE event_id=$1`, id)
	if err == nil {
		for rows.Next() {
			var c Credential
			if err := rows.Scan(&c.Kind, &c.Sha256, &c.Label); err == nil {
				d.Credentials = append(d.Credentials, c)
			}
		}
		rows.Close()
	}
	return &d, nil
}

// ExportEvents genera el contenido del export (jsonl o csv) para los eventos.
// scope: "todas" o "etapa_actual" (filtra por la fase 'activa' del ticket vigente).
func (s *Store) ExportEvents(ctx context.Context, scope, format string) (content, contentType, filename string, err error) {
	f := EventFilters{Limit: 1000}
	if scope == "etapa_actual" {
		if tid, ok := s.latestTicketID(ctx); ok {
			var num *int
			_ = s.pool.QueryRow(ctx, `SELECT number FROM stages WHERE ticket_id=$1 AND status='activa' LIMIT 1`, tid).Scan(&num)
			f.Stage = num
		}
	}
	events, err := s.EventsFiltered(ctx, f)
	if err != nil {
		return "", "", "", err
	}
	stamp := time.Now().UTC().Format("20060102-150405")
	if format == "csv" {
		var b strings.Builder
		b.WriteString("ts,agent_role,type,tool,is_error,is_repetition,summary\n")
		for _, e := range events {
			b.WriteString(fmt.Sprintf("%s,%s,%s,%s,%t,%t,%q\n",
				e.Ts.Format(time.RFC3339), e.AgentRole, e.Type, deref(e.Tool), e.IsError, e.IsRepetition, deref(e.Summary)))
		}
		return b.String(), "text/csv", "onix-eventos-" + stamp + ".csv", nil
	}
	// jsonl (default)
	var b strings.Builder
	for _, e := range events {
		line, _ := jsonLine(e)
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String(), "application/x-ndjson", "onix-eventos-" + stamp + ".jsonl", nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
