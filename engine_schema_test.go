package durable

import (
	"database/sql"
	"strings"
	"testing"
)

func TestSchemaTablesExist(t *testing.T) {
	e := newTestEngine(t)

	rows, err := e.db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := map[string]bool{
		"workflows":       false,
		"workflow_events": false,
		"workflow_steps":  false,
		"workflow_signals": false,
		"workflow_timers": false,
	}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if _, ok := expected[name]; ok {
			expected[name] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	for table, found := range expected {
		if !found {
			t.Errorf("table %q does not exist", table)
		}
	}
}

func TestSchemaIndexesExist(t *testing.T) {
	e := newTestEngine(t)

	rows, err := e.db.Query(`SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := map[string]bool{
		"idx_events_workflow": false,
		"idx_signals_workflow": false,
		"idx_timers_wake":     false,
	}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if _, ok := expected[name]; ok {
			expected[name] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	for idx, found := range expected {
		if !found {
			t.Errorf("index %q does not exist", idx)
		}
	}
}

func TestSchemaWorkflowsColumns(t *testing.T) {
	e := newTestEngine(t)

	expected := []struct {
		name     string
		nullable bool
	}{
		{"workflow_id", false},
		{"workflow_type", false},
		{"status", false},
		{"current_step", false},
		{"lease_owner", true},
		{"lease_expires_at", true},
		{"created_at", false},
		{"updated_at", false},
		{"completed_at", true},
		{"version", false},
	}

	verifyColumns(t, e.db, "workflows", expected)
}

func TestSchemaWorkflowEventsColumns(t *testing.T) {
	e := newTestEngine(t)

	expected := []struct {
		name     string
		nullable bool
	}{
		{"event_id", true}, // AUTOINCREMENT makes this nullable in sqlite
		{"workflow_id", false},
		{"event_type", false},
		{"payload_json", true},
		{"sequence_number", false},
		{"created_at", false},
	}

	verifyColumns(t, e.db, "workflow_events", expected)
}

func TestSchemaWorkflowStepsColumns(t *testing.T) {
	e := newTestEngine(t)

	expected := []struct {
		name     string
		nullable bool
	}{
		{"workflow_id", false},
		{"step_number", false},
		{"status", false},
		{"input_json", true},
		{"output_json", true},
		{"started_at", true},
		{"completed_at", true},
	}

	verifyColumns(t, e.db, "workflow_steps", expected)
}

func TestSchemaWorkflowSignalsColumns(t *testing.T) {
	e := newTestEngine(t)

	expected := []struct {
		name     string
		nullable bool
	}{
		{"signal_id", true}, // AUTOINCREMENT makes this nullable in sqlite
		{"workflow_id", false},
		{"signal_type", false},
		{"payload_json", true},
		{"consumed", false},
		{"created_at", false},
	}

	verifyColumns(t, e.db, "workflow_signals", expected)
}

func TestSchemaWorkflowTimersColumns(t *testing.T) {
	e := newTestEngine(t)

	expected := []struct {
		name     string
		nullable bool
	}{
		{"timer_id", true}, // AUTOINCREMENT makes this nullable in sqlite
		{"workflow_id", false},
		{"wake_at", false},
		{"fired", false},
	}

	verifyColumns(t, e.db, "workflow_timers", expected)
}

func TestSchemaDefaults(t *testing.T) {
	e := newTestEngine(t)

	tests := []struct {
		table  string
		column string
		substr string
	}{
		{"workflows", "status", "QUEUED"},
		{"workflows", "current_step", "0"},
		{"workflows", "version", "1"},
		{"workflow_signals", "consumed", "0"},
		{"workflow_timers", "fired", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.table+"/"+tt.column, func(t *testing.T) {
			var dflt sql.NullString
			err := e.db.QueryRow(
				`SELECT dflt_value FROM pragma_table_info(?) WHERE name=?`, tt.table, tt.column,
			).Scan(&dflt)
			if err != nil {
				t.Fatal(err)
			}
			if !dflt.Valid || !strings.Contains(dflt.String, tt.substr) {
				t.Errorf("expected %q default containing %q, got %q", tt.column, tt.substr, dflt.String)
			}
		})
	}
}

func TestSchemaForeignKeyEnforcement(t *testing.T) {
	e := newTestEngine(t)

	_, err := e.db.Exec(`INSERT INTO workflow_events (workflow_id, event_type, sequence_number, created_at) VALUES ('nonexistent', 'test', 1, 123)`)
	if err == nil {
		t.Error("expected foreign key error when inserting event for nonexistent workflow")
	}

	_, err = e.db.Exec(`INSERT INTO workflow_steps (workflow_id, step_number, status) VALUES ('nonexistent', 1, 'RUNNING')`)
	if err == nil {
		t.Error("expected foreign key error when inserting step for nonexistent workflow")
	}

	_, err = e.db.Exec(`INSERT INTO workflow_signals (workflow_id, signal_type, created_at) VALUES ('nonexistent', 'test', 123)`)
	if err == nil {
		t.Error("expected foreign key error when inserting signal for nonexistent workflow")
	}

	_, err = e.db.Exec(`INSERT INTO workflow_timers (workflow_id, wake_at) VALUES ('nonexistent', 123)`)
	if err == nil {
		t.Error("expected foreign key error when inserting timer for nonexistent workflow")
	}
}

func TestSchemaPragmaJournalMode(t *testing.T) {
	e := newTestEngine(t)

	var mode string
	err := e.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("expected journal_mode=wal, got %s", mode)
	}
}

func TestSchemaPragmaForeignKeys(t *testing.T) {
	e := newTestEngine(t)

	var on int
	err := e.db.QueryRow(`PRAGMA foreign_keys`).Scan(&on)
	if err != nil {
		t.Fatal(err)
	}
	if on != 1 {
		t.Errorf("expected foreign_keys=ON, got %d", on)
	}
}

func TestSchemaPragmaBusyTimeout(t *testing.T) {
	e := newTestEngine(t)

	var timeout int
	err := e.db.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout)
	if err != nil {
		t.Fatal(err)
	}
	if timeout != 5000 {
		t.Errorf("expected busy_timeout=5000, got %d", timeout)
	}
}

func TestSchemaStrictTables(t *testing.T) {
	e := newTestEngine(t)

	rows, err := e.db.Query(`SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, sqlText string
		if err := rows.Scan(&name, &sqlText); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(sqlText, "STRICT") {
			t.Errorf("table %q is not STRICT", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func verifyColumns(t *testing.T, db *sql.DB, table string, expected []struct {
	name     string
	nullable bool
}) {
	t.Helper()
	rows, err := db.Query(`SELECT name, "notnull" FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	got := make(map[string]bool)
	for rows.Next() {
		var name string
		var notnull int
		if err := rows.Scan(&name, &notnull); err != nil {
			t.Fatal(err)
		}
		got[name] = notnull != 0
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	for _, col := range expected {
		nullable, exists := got[col.name]
		if !exists {
			t.Errorf("table %q: column %q missing", table, col.name)
			continue
		}
		if col.nullable && nullable {
			t.Errorf("table %q: column %q should be nullable", table, col.name)
		}
		if !col.nullable && !nullable {
			t.Errorf("table %q: column %q should not be nullable", table, col.name)
		}
	}
}
