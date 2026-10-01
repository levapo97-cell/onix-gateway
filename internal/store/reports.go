package store

import (
	"context"
	"encoding/json"
	"time"
)

// ReportListItem: fila de la bandeja de Reportes.
type ReportListItem struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	AgentRole string    `json:"agent_role"`
	Stage     *int      `json:"stage"`
	Summary   *string   `json:"summary"`
	CreatedAt time.Time `json:"created_at"`
}

// ReportMessage: un mensaje del chat del reporte.
type ReportMessage struct {
	Sender    string    `json:"sender"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// ReportDetail: el reporte completo + su chat.
type ReportDetail struct {
	ReportListItem
	Deliverables []string        `json:"deliverables"`
	Decisions    []string        `json:"decisions"`
	Tokens       *int            `json:"tokens"`
	CostUsd      *float64        `json:"cost_usd"`
	Errors       *int            `json:"errors"`
	Repetitions  *int            `json:"repetitions"`
	Messages     []ReportMessage `json:"messages"`
}

func (s *Store) Reports(ctx context.Context, limit int) ([]ReportListItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.title, r.status, a.role, st.number, r.summary, r.created_at
		FROM reports r
		JOIN agents a ON a.id = r.agent_id
		LEFT JOIN stages st ON st.id = r.stage_id
		ORDER BY r.created_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ReportListItem, 0, limit)
	for rows.Next() {
		var r ReportListItem
		if err := rows.Scan(&r.ID, &r.Title, &r.Status, &r.AgentRole, &r.Stage, &r.Summary, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Report(ctx context.Context, id string) (*ReportDetail, error) {
	var d ReportDetail
	var deliverables, decisions []byte
	err := s.pool.QueryRow(ctx, `
		SELECT r.id, r.title, r.status, a.role, st.number, r.summary, r.created_at,
		       r.deliverables, r.decisions, r.tokens, r.cost_usd, r.errors, r.repetitions
		FROM reports r
		JOIN agents a ON a.id = r.agent_id
		LEFT JOIN stages st ON st.id = r.stage_id
		WHERE r.id = $1`, id).Scan(
		&d.ID, &d.Title, &d.Status, &d.AgentRole, &d.Stage, &d.Summary, &d.CreatedAt,
		&deliverables, &decisions, &d.Tokens, &d.CostUsd, &d.Errors, &d.Repetitions,
	)
	if err != nil {
		return nil, err
	}
	d.Deliverables = toStrSlice(deliverables)
	d.Decisions = toStrSlice(decisions)

	msgs, err := s.pool.Query(ctx, `SELECT sender, body, created_at FROM messages WHERE report_id=$1 ORDER BY created_at`, id)
	if err != nil {
		return nil, err
	}
	defer msgs.Close()
	d.Messages = []ReportMessage{}
	for msgs.Next() {
		var m ReportMessage
		if err := msgs.Scan(&m.Sender, &m.Body, &m.CreatedAt); err != nil {
			return nil, err
		}
		d.Messages = append(d.Messages, m)
	}
	return &d, nil
}

func toStrSlice(raw []byte) []string {
	if len(raw) == 0 {
		return []string{}
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return []string{}
	}
	return out
}
