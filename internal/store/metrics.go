package store

import "context"

// Overview: métricas del header de Monitoreo (todo el proyecto).
type Overview struct {
	Tools       int     `json:"tools"`
	Tokens      int     `json:"tokens"`
	CostUsd     float64 `json:"cost_usd"`
	Errors      int     `json:"errors"`
	Repetitions int     `json:"repetitions"`
	CurrentStage int    `json:"current_stage"`
	TotalStages int     `json:"total_stages"`
}

func (s *Store) Overview(ctx context.Context) (Overview, error) {
	var o Overview
	err := s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE tool IS NOT NULL),
			coalesce(sum(tokens),0),
			coalesce(sum(cost_usd),0),
			count(*) FILTER (WHERE is_error),
			count(*) FILTER (WHERE is_repetition)
		FROM events`).Scan(&o.Tools, &o.Tokens, &o.CostUsd, &o.Errors, &o.Repetitions)
	if err != nil {
		return o, err
	}
	o.TotalStages = 12
	// etapa actual = la 'activa'; si no hay, (hechas)+1, acotado a 1..12.
	var current *int
	_ = s.pool.QueryRow(ctx, `SELECT number FROM stages WHERE status='activa' ORDER BY number LIMIT 1`).Scan(&current)
	if current != nil {
		o.CurrentStage = *current
	} else {
		var done int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM stages WHERE status='hecha'`).Scan(&done)
		o.CurrentStage = done + 1
		if o.CurrentStage > 12 {
			o.CurrentStage = 12
		}
	}
	return o, nil
}

// StageRow: una etapa del plan.
type StageRow struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

func (s *Store) StageList(ctx context.Context) ([]StageRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT number, name, status FROM stages ORDER BY number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StageRow{}
	for rows.Next() {
		var r StageRow
		if err := rows.Scan(&r.Number, &r.Name, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AgentRow: métricas por agente para las tarjetas de Monitoreo.
type AgentRow struct {
	ID          string   `json:"id"`
	Role        string   `json:"role"`
	DisplayName *string  `json:"display_name"`
	Status      string   `json:"status"`
	Events      int      `json:"events"`
	Errors      int      `json:"errors"`
	Repetitions int      `json:"repetitions"`
	Tokens      int      `json:"tokens"`
	CostUsd     float64  `json:"cost_usd"`
}

func (s *Store) AgentList(ctx context.Context) ([]AgentRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.role, a.display_name,
		       coalesce(sess.status, 'inactivo') AS status,
		       count(e.id) AS events,
		       count(e.id) FILTER (WHERE e.is_error) AS errors,
		       count(e.id) FILTER (WHERE e.is_repetition) AS repetitions,
		       coalesce(sum(e.tokens),0) AS tokens,
		       coalesce(sum(e.cost_usd),0) AS cost_usd
		FROM agents a
		LEFT JOIN events e ON e.agent_id = a.id
		LEFT JOIN LATERAL (
			SELECT status FROM sessions WHERE agent_id = a.id ORDER BY started_at DESC LIMIT 1
		) sess ON true
		GROUP BY a.id, a.role, a.display_name, sess.status
		ORDER BY a.role`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AgentRow{}
	for rows.Next() {
		var r AgentRow
		if err := rows.Scan(&r.ID, &r.Role, &r.DisplayName, &r.Status, &r.Events, &r.Errors, &r.Repetitions, &r.Tokens, &r.CostUsd); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
