package durable

import (
	"database/sql"
	"fmt"
	"time"
)

// CreateTimer schedules a wake-up for the given workflow. The caller
// should call MarkWaiting after creating the timer so the workflow
// pauses until the timer fires.
func (e *Engine) CreateTimer(workflowID string, wakeAt time.Time) (*Timer, error) {
	epoch := wakeAt.Unix()

	result, err := e.db.Exec(`
		INSERT INTO workflow_timers (workflow_id, wake_at)
		VALUES (?, ?)
	`, workflowID, epoch)
	if err != nil {
		return nil, fmt.Errorf("insert timer: %w", err)
	}

	id, _ := result.LastInsertId()
	return &Timer{
		TimerID:    id,
		WorkflowID: workflowID,
		WakeAt:     epoch,
		Fired:      false,
	}, nil
}

// fireExpiredTimers fires all timers whose wake_at has passed and
// marks the corresponding workflows as QUEUED so they can be picked
// up by workers. Called by the scheduler.
func (e *Engine) fireExpiredTimers(now int64) (int, error) {
	tx, err := e.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
		SELECT t.timer_id, t.workflow_id
		FROM workflow_timers t
		JOIN workflows w ON w.workflow_id = t.workflow_id
		WHERE t.fired = 0 AND t.wake_at <= ? AND w.status = ?
	`, now, StatusWaiting)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type pair struct {
		timerID    int64
		workflowID string
	}
	var timers []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.timerID, &p.workflowID); err != nil {
			return 0, err
		}
		timers = append(timers, p)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	count := 0
	for _, p := range timers {
		_, err = tx.Exec(`UPDATE workflow_timers SET fired = 1 WHERE timer_id = ?`, p.timerID)
		if err != nil {
			return count, err
		}

		_, err = tx.Exec(`
			UPDATE workflows
			SET status = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE workflow_id = ?
		`, StatusQueued, now, p.workflowID)
		if err != nil {
			return count, err
		}

		if err := appendEventTx(tx, p.workflowID, EventTimerFired, nil, 0, now); err != nil {
			return count, err
		}

		count++
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("commit: %w", err)
	}
	return count, nil
}

// expireLeases resets leases that have passed their expiry. The
// affected workflows are returned to QUEUED so another worker can
// claim them.
func (e *Engine) expireLeases(now int64) (int, error) {
	result, err := e.db.Exec(`
		UPDATE workflows
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
		    updated_at = ?
		WHERE lease_expires_at IS NOT NULL
		  AND lease_expires_at <= ?
		  AND status = ?
	`, StatusQueued, now, now, StatusRunning)
	if err != nil {
		return 0, fmt.Errorf("expire leases: %w", err)
	}

	n, _ := result.RowsAffected()
	return int(n), nil
}

// GetPendingTimers returns all unfired timers for a workflow.
func (e *Engine) GetPendingTimers(workflowID string) ([]Timer, error) {
	rows, err := e.db.Query(`
		SELECT timer_id, workflow_id, wake_at, fired
		FROM workflow_timers
		WHERE workflow_id = ? AND fired = 0
		ORDER BY wake_at ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var timers []Timer
	for rows.Next() {
		var t Timer
		err := rows.Scan(&t.TimerID, &t.WorkflowID, &t.WakeAt, &t.Fired)
		if err != nil {
			return nil, err
		}
		timers = append(timers, t)
	}
	return timers, rows.Err()
}

// hasExpiredTimer checks if a workflow has an unfired timer whose
// wake_at has passed.
func (e *Engine) hasExpiredTimer(workflowID string, now int64) (bool, error) {
	var exists int
	err := e.db.QueryRow(`
		SELECT 1 FROM workflow_timers
		WHERE workflow_id = ? AND fired = 0 AND wake_at <= ?
		LIMIT 1
	`, workflowID, now).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
