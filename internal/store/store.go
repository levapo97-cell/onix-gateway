// Package store lee eventos de Postgres para el endpoint /api/events (carga inicial de la web).
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

// EventRow es la forma que consume el frontend (aplanada, sin joins en el cliente).
type EventRow struct {
	ID        string    `json:"id"`
	Ts        time.Time `json:"ts"`
	Type      string    `json:"type"`
	Tool      *string   `json:"tool,omitempty"`
	Summary   *string   `json:"summary,omitempty"`
	AgentRole string    `json:"agent_role"`
	IsError   bool      `json:"is_error"`
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("conectar postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// RecentEvents devuelve los últimos eventos (para pintar la lista al abrir la web).
func (s *Store) RecentEvents(ctx context.Context, limit int) ([]EventRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.ts, e.type, e.tool, e.summary, a.role, e.is_error
		FROM events e
		JOIN agents a ON a.id = e.agent_id
		ORDER BY e.ts DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]EventRow, 0, limit)
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(&r.ID, &r.Ts, &r.Type, &r.Tool, &r.Summary, &r.AgentRole, &r.IsError); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
