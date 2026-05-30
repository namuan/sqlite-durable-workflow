package durable

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrLeaseMismatch is returned when a worker attempts to operate on a
// workflow it does not currently lease.
var ErrLeaseMismatch = errors.New("lease mismatch: worker does not own this workflow")

// ErrStepAlreadyCompleted is returned when attempting to complete a
// step that was already checkpointed (primary key violation).
var ErrStepAlreadyCompleted = errors.New("step already completed")

// StartStep records the start of a step execution and emits a
// STEP_STARTED event. The caller must hold the lease.
func (e *Engine) StartStep(workflowID, workerID string, stepNumber int, input interface{}) error {
	if err := e.verifyLease(workflowID, workerID); err != nil {
		return err
	}

	now := time.Now().Unix()
	inputStr := jsonString(input)

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT OR IGNORE INTO workflow_steps
			(workflow_id, step_number, status, input_json, started_at)
		VALUES (?, ?, ?, ?, ?)
	`, workflowID, stepNumber, "RUNNING", inputStr, now)
	if err != nil {
		return fmt.Errorf("insert step: %w", err)
	}

	if err := appendEventTx(tx, workflowID, EventStepStarted, inputStr, 0, now); err != nil {
		return err
	}

	return tx.Commit()
}

// CompleteStep checkpoints a successful step execution and advances
// the workflow cursor. All changes (checkpoint, event, cursor advance)
// happen in a single transaction. Works regardless of whether StartStep
// was called first — the PRIMARY KEY enforces exactly-once semantics.
func (e *Engine) CompleteStep(workflowID, workerID string, stepNumber int, input, output interface{}) error {
	now := time.Now().Unix()
	inputStr := jsonString(input)
	outputStr := jsonString(output)

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	// Verify lease ownership to prevent duplicate workers.
	var owner sql.NullString
	err = tx.QueryRow(
		`SELECT lease_owner FROM workflows WHERE workflow_id = ?`,
		workflowID,
	).Scan(&owner)
	if err != nil {
		return fmt.Errorf("verify lease: %w", err)
	}
	if !owner.Valid || owner.String != workerID {
		return ErrLeaseMismatch
	}

	// Upsert the checkpoint. Updates in-place if StartStep or FailStep
	// we update it in-place; otherwise we insert from scratch.
	result, err := tx.Exec(`
		INSERT INTO workflow_steps
			(workflow_id, step_number, status, input_json, output_json,
			 started_at, completed_at)
		VALUES (?, ?, 'COMPLETED', ?, ?, ?, ?)
		ON CONFLICT(workflow_id, step_number) DO UPDATE SET
			status = 'COMPLETED',
			output_json = excluded.output_json,
			completed_at = excluded.completed_at
		WHERE workflow_steps.status IN ('RUNNING', 'FAILED')
	`, workflowID, stepNumber, inputStr, outputStr, now, now)
	if err != nil {
		return fmt.Errorf("upsert step: %w", err)
	}

	// RowsAffected == 0 means the row already had status COMPLETED.
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrStepAlreadyCompleted
	}

	// Advance the workflow cursor.
	_, err = tx.Exec(`
		UPDATE workflows
		SET current_step = current_step + 1,
		    updated_at = ?, version = version + 1
		WHERE workflow_id = ? AND lease_owner = ?
	`, now, workflowID, workerID)
	if err != nil {
		return fmt.Errorf("advance cursor: %w", err)
	}

	payload := map[string]interface{}{
		"step_number": stepNumber,
		"output":      output,
	}
	payloadStr := jsonString(payload)

	if err := appendEventTx(tx, workflowID, EventStepCompleted, payloadStr, 0, now); err != nil {
		return err
	}

	return tx.Commit()
}

// FailStep records a failed step execution and emits a STEP_FAILED
// event. The workflow cursor is NOT advanced on failure.
func (e *Engine) FailStep(workflowID, workerID string, stepNumber int, input interface{}, err_ error) error {
	if err := e.verifyLease(workflowID, workerID); err != nil {
		return err
	}

	now := time.Now().Unix()
	inputStr := jsonString(input)

	payloadStr := jsonString(map[string]interface{}{
		"step_number": stepNumber,
		"error":       err_.Error(),
	})

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO workflow_steps
			(workflow_id, step_number, status, input_json, output_json,
			 started_at, completed_at)
		VALUES (?, ?, 'FAILED', ?, ?, ?, ?)
		ON CONFLICT(workflow_id, step_number) DO UPDATE SET
			status = 'FAILED',
			output_json = excluded.output_json,
			completed_at = excluded.completed_at
	`, workflowID, stepNumber, inputStr, payloadStr, now, now)
	if err != nil {
		return fmt.Errorf("upsert step: %w", err)
	}

	if err := appendEventTx(tx, workflowID, EventStepFailed, payloadStr, 0, now); err != nil {
		return err
	}

	return tx.Commit()
}

// GetSteps returns all checkpointed steps for a workflow, ordered by
// step number.
func (e *Engine) GetSteps(workflowID string) ([]WorkflowStep, error) {
	rows, err := e.db.Query(`
		SELECT workflow_id, step_number, status,
		       input_json, output_json, started_at, completed_at
		FROM workflow_steps
		WHERE workflow_id = ?
		ORDER BY step_number ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []WorkflowStep
	for rows.Next() {
		var s WorkflowStep
		err := rows.Scan(
			&s.WorkflowID, &s.StepNumber, &s.Status,
			&s.InputJSON, &s.OutputJSON, &s.StartedAt, &s.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// GetEvents returns all events for a workflow, ordered by sequence
// number.
func (e *Engine) GetEvents(workflowID string) ([]WorkflowEvent, error) {
	rows, err := e.db.Query(`
		SELECT event_id, workflow_id, event_type, payload_json,
		       sequence_number, created_at
		FROM workflow_events
		WHERE workflow_id = ?
		ORDER BY sequence_number ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []WorkflowEvent
	for rows.Next() {
		var ev WorkflowEvent
		err := rows.Scan(
			&ev.EventID, &ev.WorkflowID, &ev.EventType,
			&ev.PayloadJSON, &ev.SequenceNumber, &ev.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// appendEventTx inserts an event into workflow_events within an
// existing transaction. If sequenceNumber is 0, the next sequence
// number is determined automatically.
func appendEventTx(tx *sql.Tx, workflowID, eventType string, payloadJSON *string, sequenceNumber int, createdAt int64) error {
	if sequenceNumber == 0 {
		seq, err := nextSequenceTx(tx, workflowID)
		if err != nil {
			return fmt.Errorf("next sequence: %w", err)
		}
		sequenceNumber = seq
	}

	_, err := tx.Exec(`
		INSERT INTO workflow_events
			(workflow_id, event_type, payload_json, sequence_number, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, workflowID, eventType, payloadJSON, sequenceNumber, createdAt)
	return err
}

func nextSequenceTx(tx *sql.Tx, workflowID string) (int, error) {
	var maxSeq sql.NullInt64
	err := tx.QueryRow(`
		SELECT MAX(sequence_number)
		FROM workflow_events
		WHERE workflow_id = ?
	`, workflowID).Scan(&maxSeq)
	if err != nil {
		return 0, err
	}
	if maxSeq.Valid {
		return int(maxSeq.Int64) + 1, nil
	}
	return 1, nil
}


