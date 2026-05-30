package durable

import (
	"database/sql"
	"fmt"
	"time"
)

// SendSignal persists an asynchronous signal for the given workflow
// and emits a SIGNAL_RECEIVED event.
func (e *Engine) SendSignal(workflowID, signalType string, payload interface{}) (*Signal, error) {
	now := time.Now().Unix()
	payloadStr := jsonString(payload)

	tx, err := e.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(`
		INSERT INTO workflow_signals
			(workflow_id, signal_type, payload_json, created_at)
		VALUES (?, ?, ?, ?)
	`, workflowID, signalType, payloadStr, now)
	if err != nil {
		return nil, fmt.Errorf("insert signal: %w", err)
	}

	signalID, _ := result.LastInsertId()

	eventPayload := jsonString(map[string]interface{}{
		"signal_id":   signalID,
		"signal_type": signalType,
	})
	if err := appendEventTx(tx, workflowID, EventSignalReceived, eventPayload, 0, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return &Signal{
		SignalID:    signalID,
		WorkflowID:  workflowID,
		SignalType:  signalType,
		PayloadJSON: payloadStr,
		Consumed:    false,
		CreatedAt:   now,
	}, nil
}

// ConsumeSignal atomically consumes the oldest unconsumed signal for
// a workflow. Returns nil, nil if no signal is available.
func (e *Engine) ConsumeSignal(workflowID string) (*Signal, error) {
	tx, err := e.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	var s Signal
	var consumed int
	err = tx.QueryRow(`
		SELECT signal_id, workflow_id, signal_type, payload_json, consumed, created_at
		FROM workflow_signals
		WHERE workflow_id = ? AND consumed = 0
		ORDER BY created_at ASC
		LIMIT 1
	`, workflowID).Scan(
		&s.SignalID, &s.WorkflowID, &s.SignalType,
		&s.PayloadJSON, &consumed, &s.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("select: %w", err)
	}
	s.Consumed = consumed != 0

	_, err = tx.Exec(`
		UPDATE workflow_signals SET consumed = 1
		WHERE signal_id = ?
	`, s.SignalID)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	s.Consumed = true
	return &s, nil
}

// GetSignals returns all signals for a workflow, ordered by creation
// time. If unconsumedOnly is true, only pending signals are returned.
func (e *Engine) GetSignals(workflowID string, unconsumedOnly bool) ([]Signal, error) {
	query := `
		SELECT signal_id, workflow_id, signal_type, payload_json, consumed, created_at
		FROM workflow_signals
		WHERE workflow_id = ?
	`
	if unconsumedOnly {
		query += " AND consumed = 0"
	}
	query += " ORDER BY created_at ASC"

	rows, err := e.db.Query(query, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var signals []Signal
	for rows.Next() {
		var s Signal
		var consumed int
		err := rows.Scan(
			&s.SignalID, &s.WorkflowID, &s.SignalType,
			&s.PayloadJSON, &consumed, &s.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		s.Consumed = consumed != 0
		signals = append(signals, s)
	}
	return signals, rows.Err()
}
