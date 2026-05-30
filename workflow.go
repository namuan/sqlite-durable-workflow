package durable

import (
	"database/sql"
	"fmt"
	"time"
)

// CreateWorkflow inserts a new workflow in QUEUED status, emits a
// WORKFLOW_CREATED event, and returns the created workflow.
func (e *Engine) CreateWorkflow(workflowID, workflowType string) (*Workflow, error) {
	now := time.Now().Unix()

	tx, err := e.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO workflows (workflow_id, workflow_type, status, current_step,
		                       created_at, updated_at, version)
		VALUES (?, ?, ?, 0, ?, ?, 1)
	`, workflowID, workflowType, StatusQueued, now, now)
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	if err := appendEventTx(tx, workflowID, EventWorkflowCreated, nil, 1, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return e.GetWorkflow(workflowID)
}

// GetWorkflow returns a workflow by ID, or sql.ErrNoRows if not found.
func (e *Engine) GetWorkflow(workflowID string) (*Workflow, error) {
	var w Workflow
	err := e.db.QueryRow(`
		SELECT workflow_id, workflow_type, status, current_step,
		       lease_owner, lease_expires_at,
		       created_at, updated_at, completed_at, version
		FROM workflows
		WHERE workflow_id = ?
	`, workflowID).Scan(
		&w.WorkflowID, &w.WorkflowType, &w.Status, &w.CurrentStep,
		&w.LeaseOwner, &w.LeaseExpiresAt,
		&w.CreatedAt, &w.UpdatedAt, &w.CompletedAt, &w.Version,
	)
	if err != nil {
		return nil, err
	}
	return &w, nil
}

// AcquireLease attempts to lease the next eligible QUEUED workflow to
// the given workerID. Returns nil, nil if no work is available.
func (e *Engine) AcquireLease(workerID string, leaseDuration time.Duration) (*Workflow, error) {
	now := time.Now().Unix()
	expiresAt := now + int64(leaseDuration.Seconds())

	tx, err := e.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	// Select one eligible workflow: QUEUED, or WAITING with expired timer,
	// or any workflow whose lease has expired.
	row := tx.QueryRow(`
		SELECT workflow_id FROM workflows
		WHERE (status = ?)
		   OR (status = ? AND workflow_id IN (
		         SELECT workflow_id FROM workflow_timers
		         WHERE fired = 0 AND wake_at <= ?
		       ))
		   OR (lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
		ORDER BY created_at ASC
		LIMIT 1
	`, StatusQueued, StatusWaiting, now, now)

	var workflowID string
	if err := row.Scan(&workflowID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("select: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE workflows
		SET status = ?, lease_owner = ?, lease_expires_at = ?,
		    updated_at = ?, version = version + 1
		WHERE workflow_id = ?
	`, StatusRunning, workerID, expiresAt, now, workflowID)
	if err != nil {
		return nil, fmt.Errorf("update lease: %w", err)
	}

	if err := appendEventTx(tx, workflowID, EventWorkflowStarted, nil, 0, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return e.GetWorkflow(workflowID)
}

// RenewLease extends the lease for a workflow owned by workerID.
// Returns ErrLeaseMismatch if the worker does not hold the lease.
func (e *Engine) RenewLease(workflowID, workerID string, leaseDuration time.Duration) error {
	now := time.Now().Unix()
	expiresAt := now + int64(leaseDuration.Seconds())

	result, err := e.db.Exec(`
		UPDATE workflows
		SET lease_expires_at = ?, updated_at = ?
		WHERE workflow_id = ? AND lease_owner = ? AND status = ?
	`, expiresAt, now, workflowID, workerID, StatusRunning)
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// ReleaseLease releases the lease on a workflow owned by workerID,
// returning it to QUEUED status.
func (e *Engine) ReleaseLease(workflowID, workerID string) error {
	now := time.Now().Unix()

	result, err := e.db.Exec(`
		UPDATE workflows
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
		    updated_at = ?
		WHERE workflow_id = ? AND lease_owner = ?
	`, StatusQueued, now, workflowID, workerID)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// CompleteWorkflow marks a workflow as COMPLETED, emits the terminal
// event, and releases the lease. Returns ErrLeaseMismatch if the
// worker does not hold the lease.
func (e *Engine) CompleteWorkflow(workflowID, workerID string) error {
	now := time.Now().Unix()

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(`
		UPDATE workflows
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
		    completed_at = ?, updated_at = ?, version = version + 1
		WHERE workflow_id = ? AND lease_owner = ? AND status = ?
	`, StatusCompleted, now, now, workflowID, workerID, StatusRunning)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrLeaseMismatch
	}

	if err := appendEventTx(tx, workflowID, EventWorkflowCompleted, nil, 0, now); err != nil {
		return err
	}

	return tx.Commit()
}

// FailWorkflow marks a workflow as FAILED, emits the terminal event,
// and releases the lease.
func (e *Engine) FailWorkflow(workflowID, workerID string) error {
	now := time.Now().Unix()

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(`
		UPDATE workflows
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
		    completed_at = ?, updated_at = ?, version = version + 1
		WHERE workflow_id = ? AND lease_owner = ? AND status = ?
	`, StatusFailed, now, now, workflowID, workerID, StatusRunning)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrLeaseMismatch
	}

	if err := appendEventTx(tx, workflowID, EventWorkflowFailed, nil, 0, now); err != nil {
		return err
	}

	return tx.Commit()
}

// MarkWaiting transitions a workflow to WAITING status (e.g. waiting
// for a timer or signal) while preserving the lease.
func (e *Engine) MarkWaiting(workflowID, workerID string) error {
	now := time.Now().Unix()

	result, err := e.db.Exec(`
		UPDATE workflows
		SET status = ?, updated_at = ?
		WHERE workflow_id = ? AND lease_owner = ? AND status = ?
	`, StatusWaiting, now, workflowID, workerID, StatusRunning)
	if err != nil {
		return fmt.Errorf("mark waiting: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// verifyLease checks that workerID holds the lease on workflowID.
func (e *Engine) verifyLease(workflowID, workerID string) error {
	var owner sql.NullString
	err := e.db.QueryRow(`
		SELECT lease_owner FROM workflows WHERE workflow_id = ?
	`, workflowID).Scan(&owner)
	if err != nil {
		return err
	}
	if !owner.Valid || owner.String != workerID {
		return ErrLeaseMismatch
	}
	return nil
}
