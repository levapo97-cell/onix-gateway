package store

import "context"

// PostMortem: informe agregado para el estudio multiagente (§8 del plan).
// Calculado desde Postgres (el gateway lo expone; onix-guard podría enriquecerlo con narrativa después).
type PostMortem struct {
	Ticket     *string          `json:"ticket"`
	ByStage    []StageAgg       `json:"by_stage"`
	ByAgent    []AgentAgg       `json:"by_agent"`
	Totals     Totals           `json:"totals"`
	Conclusion string           `json:"conclusion"`
}

type StageAgg struct {
	Number      int     `json:"number"`
	Name        string  `json:"name"`
	Status      string  `json:"status"`
	Events      int     `json:"events"`
	Errors      int     `json:"errors"`
	Repetitions int     `json:"repetitions"`
	Tokens      int     `json:"tokens"`
	CostUsd     float64 `json:"cost_usd"`
	DurationMs  int64   `json:"duration_ms"`
}

type AgentAgg struct {
	Role        string  `json:"role"`
	Events      int     `json:"events"`
	Errors      int     `json:"errors"`
	Repetitions int     `json:"repetitions"`
	Tokens      int     `json:"tokens"`
	CostUsd     float64 `json:"cost_usd"`
	DurationMs  int64   `json:"duration_ms"`
}

type Totals struct {
	Events      int     `json:"events"`
	Errors      int     `json:"errors"`
	Repetitions int     `json:"repetitions"`
	Tokens      int     `json:"tokens"`
	CostUsd     float64 `json:"cost_usd"`
	DurationMs  int64   `json:"duration_ms"`
	Stages      int     `json:"stages"`
	Agents      int     `json:"agents"`
}

func (s *Store) PostMortem(ctx context.Context) (PostMortem, error) {
	var pm PostMortem
	pm.ByStage = []StageAgg{}
	pm.ByAgent = []AgentAgg{}

	// Por fase (del ticket vigente, si hay).
	if tid, ok := s.latestTicketID(ctx); ok {
		pm.Ticket = &tid
		rows, err := s.pool.Query(ctx, `
			SELECT st.number, st.name, st.status,
			       count(e.id), count(e.id) FILTER (WHERE e.is_error), count(e.id) FILTER (WHERE e.is_repetition),
			       coalesce(sum(e.tokens),0), coalesce(sum(e.cost_usd),0), coalesce(sum(e.duration_ms),0)
			FROM stages st LEFT JOIN events e ON e.stage_id = st.id
			WHERE st.ticket_id = $1
			GROUP BY st.number, st.name, st.status ORDER BY st.number`, tid)
		if err != nil {
			return pm, err
		}
		for rows.Next() {
			var a StageAgg
			if err := rows.Scan(&a.Number, &a.Name, &a.Status, &a.Events, &a.Errors, &a.Repetitions, &a.Tokens, &a.CostUsd, &a.DurationMs); err != nil {
				rows.Close()
				return pm, err
			}
			pm.ByStage = append(pm.ByStage, a)
		}
		rows.Close()
	}

	// Por agente (ranking por errores).
	rows, err := s.pool.Query(ctx, `
		SELECT a.role, count(e.id), count(e.id) FILTER (WHERE e.is_error), count(e.id) FILTER (WHERE e.is_repetition),
		       coalesce(sum(e.tokens),0), coalesce(sum(e.cost_usd),0), coalesce(sum(e.duration_ms),0)
		FROM agents a LEFT JOIN events e ON e.agent_id = a.id
		GROUP BY a.role ORDER BY count(e.id) FILTER (WHERE e.is_error) DESC, count(e.id) DESC`)
	if err != nil {
		return pm, err
	}
	for rows.Next() {
		var a AgentAgg
		if err := rows.Scan(&a.Role, &a.Events, &a.Errors, &a.Repetitions, &a.Tokens, &a.CostUsd, &a.DurationMs); err != nil {
			rows.Close()
			return pm, err
		}
		pm.ByAgent = append(pm.ByAgent, a)
	}
	rows.Close()

	// Totales.
	_ = s.pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE is_error), count(*) FILTER (WHERE is_repetition),
		       coalesce(sum(tokens),0), coalesce(sum(cost_usd),0), coalesce(sum(duration_ms),0)
		FROM events`).Scan(&pm.Totals.Events, &pm.Totals.Errors, &pm.Totals.Repetitions, &pm.Totals.Tokens, &pm.Totals.CostUsd, &pm.Totals.DurationMs)
	pm.Totals.Stages = len(pm.ByStage)
	for _, a := range pm.ByAgent {
		if a.Events > 0 {
			pm.Totals.Agents++
		}
	}

	pm.Conclusion = conclusion(pm.Totals)
	return pm, nil
}

func conclusion(t Totals) string {
	if t.Events == 0 {
		return "Aún no hay actividad suficiente para un post-mortem."
	}
	rate := 0.0
	if t.Events > 0 {
		rate = float64(t.Errors) / float64(t.Events) * 100
	}
	verdict := "El equipo avanzó con una tasa de error baja."
	if rate > 20 {
		verdict = "Tasa de error alta: conviene revisar qué falló y afinar las skills."
	} else if t.Repetitions > t.Events/5 && t.Events > 0 {
		verdict = "Hay repeticiones notables: posible trabajo redundante que afinar."
	}
	return verdict
}
