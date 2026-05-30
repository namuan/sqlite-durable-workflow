package durable

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Engine is the core durable workflow engine backed by SQLite.
// It is safe for concurrent use by multiple goroutines.
type Engine struct {
	db *sql.DB
}

// New opens (or creates) a SQLite database at dbPath and initializes
// the workflow schema. Recommended PRAGMAs are applied automatically.
func New(dbPath string) (*Engine, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	db.SetMaxOpenConns(1)

	if err := applyPragmas(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("pragmas: %w", err)
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}

	return &Engine{db: db}, nil
}

func applyPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

func initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS workflows (
		workflow_id     TEXT PRIMARY KEY,
		workflow_type   TEXT NOT NULL,
		status          TEXT NOT NULL DEFAULT 'QUEUED',
		current_step    INTEGER NOT NULL DEFAULT 0,
		lease_owner     TEXT,
		lease_expires_at INTEGER,
		created_at      INTEGER NOT NULL,
		updated_at      INTEGER NOT NULL,
		completed_at    INTEGER,
		version         INTEGER NOT NULL DEFAULT 1
	) STRICT;

	CREATE TABLE IF NOT EXISTS workflow_events (
		event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
		workflow_id     TEXT NOT NULL,
		event_type      TEXT NOT NULL,
		payload_json    TEXT,
		sequence_number INTEGER NOT NULL,
		created_at      INTEGER NOT NULL,
		FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
	) STRICT;

	CREATE INDEX IF NOT EXISTS idx_events_workflow
		ON workflow_events(workflow_id, sequence_number);

	CREATE TABLE IF NOT EXISTS workflow_steps (
		workflow_id     TEXT NOT NULL,
		step_number     INTEGER NOT NULL,
		status          TEXT NOT NULL,
		input_json      TEXT,
		output_json     TEXT,
		started_at      INTEGER,
		completed_at    INTEGER,
		PRIMARY KEY (workflow_id, step_number),
		FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
	) STRICT;

	CREATE TABLE IF NOT EXISTS workflow_signals (
		signal_id       INTEGER PRIMARY KEY AUTOINCREMENT,
		workflow_id     TEXT NOT NULL,
		signal_type     TEXT NOT NULL,
		payload_json    TEXT,
		consumed        INTEGER NOT NULL DEFAULT 0,
		created_at      INTEGER NOT NULL,
		FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
	) STRICT;

	CREATE INDEX IF NOT EXISTS idx_signals_workflow
		ON workflow_signals(workflow_id, consumed);

	CREATE TABLE IF NOT EXISTS workflow_timers (
		timer_id        INTEGER PRIMARY KEY AUTOINCREMENT,
		workflow_id     TEXT NOT NULL,
		wake_at         INTEGER NOT NULL,
		fired           INTEGER NOT NULL DEFAULT 0,
		FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
	) STRICT;

	CREATE INDEX IF NOT EXISTS idx_timers_wake
		ON workflow_timers(wake_at, fired);
	`
	_, err := db.Exec(schema)
	return err
}

// Close closes the underlying database connection.
func (e *Engine) Close() error {
	return e.db.Close()
}

// DB returns the underlying *sql.DB for advanced querying.
func (e *Engine) DB() *sql.DB {
	return e.db
}

// QueryFailed returns all workflows with status FAILED.
func (e *Engine) QueryFailed() ([]Workflow, error) {
	return e.queryWorkflows("WHERE status = ?", StatusFailed)
}

// QueryRunning returns all workflows with status RUNNING.
func (e *Engine) QueryRunning() ([]Workflow, error) {
	return e.queryWorkflows("WHERE status = ?", StatusRunning)
}

// QueryQueued returns all workflows with status QUEUED.
func (e *Engine) QueryQueued() ([]Workflow, error) {
	return e.queryWorkflows("WHERE status = ?", StatusQueued)
}

// QueryWaiting returns all workflows with status WAITING.
func (e *Engine) QueryWaiting() ([]Workflow, error) {
	return e.queryWorkflows("WHERE status = ?", StatusWaiting)
}

func (e *Engine) queryWorkflows(whereClause string, args ...interface{}) ([]Workflow, error) {
	rows, err := e.db.Query(
		`SELECT workflow_id, workflow_type, status, current_step,
		        lease_owner, lease_expires_at,
		        created_at, updated_at, completed_at, version
		 FROM workflows `+whereClause,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workflows []Workflow
	for rows.Next() {
		var w Workflow
		err := rows.Scan(
			&w.WorkflowID, &w.WorkflowType, &w.Status, &w.CurrentStep,
			&w.LeaseOwner, &w.LeaseExpiresAt,
			&w.CreatedAt, &w.UpdatedAt, &w.CompletedAt, &w.Version,
		)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, w)
	}
	return workflows, rows.Err()
}

// AvgStepDuration returns the average duration per step number across
// all completed steps.
func (e *Engine) AvgStepDuration() (map[int]float64, error) {
	rows, err := e.db.Query(`
		SELECT step_number, AVG(completed_at - started_at)
		FROM workflow_steps
		WHERE started_at IS NOT NULL AND completed_at IS NOT NULL
		GROUP BY step_number
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int]float64)
	for rows.Next() {
		var step int
		var avg float64
		if err := rows.Scan(&step, &avg); err != nil {
			return nil, err
		}
		result[step] = avg
	}
	return result, rows.Err()
}
