package store

import "context"

// SetAttachment registra la ruta del archivo adjunto de un ticket y, si se pasó contenido de
// texto (plan en .md/.txt/.json), lo AÑADE al body del ticket para que el agente lo lea con
// consultar_ticket y arme las fases del plan a partir de él.
func (s *Store) SetAttachment(ctx context.Context, ticketID, filename, path, textContent string) error {
	if textContent != "" {
		_, err := s.pool.Exec(ctx,
			`UPDATE tickets
			 SET attachment_path = $2,
			     body = coalesce(body,'') || E'\n\n--- Adjunto: ' || $3 || E' ---\n' || $4
			 WHERE id = $1`,
			ticketID, path, filename, textContent)
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE tickets SET attachment_path = $2 WHERE id = $1`, ticketID, path)
	return err
}

// TicketExists comprueba que el ticket existe (para validar antes de subir).
func (s *Store) TicketExists(ctx context.Context, ticketID string) bool {
	var one int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM tickets WHERE id=$1`, ticketID).Scan(&one)
	return err == nil
}
