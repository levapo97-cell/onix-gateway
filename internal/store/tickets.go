package store

import (
	"context"
	"time"
)

type TicketRow struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"created_at"`
}

type TicketDetail struct {
	TicketRow
	Body   *string    `json:"body"`
	Stages []StageRow `json:"stages"`
}

// EnsureProject: get-or-create del proyecto (un solo proyecto por ahora).
func (s *Store) EnsureProject(ctx context.Context, name string) (string, error) {
	if name == "" {
		name = "onixguard"
	}
	var id string
	if err := s.pool.QueryRow(ctx, `SELECT id FROM projects WHERE name=$1 LIMIT 1`, name).Scan(&id); err == nil {
		return id, nil
	}
	err := s.pool.QueryRow(ctx, `INSERT INTO projects (name) VALUES ($1) RETURNING id`, name).Scan(&id)
	return id, err
}

func (s *Store) CreateTicket(ctx context.Context, title, body, source string) (string, error) {
	pid, err := s.EnsureProject(ctx, "onixguard")
	if err != nil {
		return "", err
	}
	if source == "" {
		source = "plataforma"
	}
	var id string
	err = s.pool.QueryRow(ctx,
		`INSERT INTO tickets (project_id, title, body, source) VALUES ($1,$2,$3,$4) RETURNING id`,
		pid, title, body, source).Scan(&id)
	return id, err
}

func (s *Store) Tickets(ctx context.Context) ([]TicketRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, title, status, source, created_at FROM tickets ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TicketRow{}
	for rows.Next() {
		var t TicketRow
		if err := rows.Scan(&t.ID, &t.Title, &t.Status, &t.Source, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) TicketDetail(ctx context.Context, id string) (*TicketDetail, error) {
	var d TicketDetail
	if err := s.pool.QueryRow(ctx,
		`SELECT id, title, status, source, created_at, body FROM tickets WHERE id=$1`, id).
		Scan(&d.ID, &d.Title, &d.Status, &d.Source, &d.CreatedAt, &d.Body); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT number, name, status FROM stages WHERE ticket_id=$1 ORDER BY number`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	d.Stages = []StageRow{}
	for rows.Next() {
		var st StageRow
		if err := rows.Scan(&st.Number, &st.Name, &st.Status); err != nil {
			return nil, err
		}
		d.Stages = append(d.Stages, st)
	}
	return &d, nil
}

// latestTicketID: el ticket más reciente (para Monitoreo: su plan es el "vigente").
func (s *Store) latestTicketID(ctx context.Context) (string, bool) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM tickets ORDER BY created_at DESC LIMIT 1`).Scan(&id)
	return id, err == nil
}
