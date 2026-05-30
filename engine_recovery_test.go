package durable

import (
	"errors"
	"testing"
	"time"
)

func TestRecoveryAfterCrashWithLease(t *testing.T) {
	path := t.TempDir() + "/recovery-lease.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-l-001", "t")
	wf, _ := e1.AcquireLease("w1", 10*time.Millisecond)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	e1.Close()

	time.Sleep(50 * time.Millisecond)

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	s := NewScheduler(e2, 100*time.Millisecond)
	s.Tick()

	wf, err = e2.GetWorkflow("rec-l-001")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after recovery, got %s (owner=%v)", wf.Status, wf.LeaseOwner)
	}

	steps, _ := e2.GetSteps("rec-l-001")
	if len(steps) != 1 {
		t.Errorf("expected 1 checkpoint after recovery, got %d", len(steps))
	}

	events, _ := e2.GetEvents("rec-l-001")
	if len(events) < 3 {
		t.Errorf("expected at least 3 events, got %d", len(events))
	}

	wf2, _ := e2.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Error("expected to re-acquire after recovery")
	} else if wf2.WorkflowID != "rec-l-001" {
		t.Errorf("expected rec-l-001, got %s", wf2.WorkflowID)
	}
}

func TestRecoveryMidStepExecution(t *testing.T) {
	path := t.TempDir() + "/recovery-mid.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-mid-001", "t")
	wf, _ := e1.AcquireLease("w1", 10*time.Second)

	e1.StartStep(wf.WorkflowID, "w1", 1, map[string]string{"input": "process"})
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	time.Sleep(100 * time.Millisecond)

	wf, err = e2.GetWorkflow("rec-mid-001")
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e2.GetSteps("rec-mid-001")
	if len(steps) != 1 {
		t.Errorf("expected 1 step checkpoint, got %d", len(steps))
	}
	if steps[0].Status != "RUNNING" {
		t.Errorf("expected RUNNING status, got %s", steps[0].Status)
	}
	if steps[0].InputJSON == nil {
		t.Error("expected input_json to be preserved")
	}
}

func TestRecoveryMultipleWorkflows(t *testing.T) {
	path := t.TempDir() + "/recovery-multi.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		id := string(rune('A' + i))
		_, err := e1.CreateWorkflow("rec-m-"+id, "t")
		if err != nil {
			t.Fatal(err)
		}
	}

	wf, _ := e1.AcquireLease("w1", 10*time.Second)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e1.CompleteWorkflow(wf.WorkflowID, "w1")

	wf, _ = e1.AcquireLease("w1", 10*time.Second)
	e1.FailStep(wf.WorkflowID, "w1", 1, nil, errors.New("recovery error"))
	e1.SendSignal(wf.WorkflowID, "alert", map[string]string{"msg": "help"})
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	queued, _ := e2.QueryQueued()
	failed, _ := e2.QueryFailed()
	running, _ := e2.QueryRunning()
	completed, _ := e2.QueryCompleted()

	if len(queued)+len(completed)+len(running)+len(failed) < 9 {
		t.Errorf("expected at least 9 workflows preserved, got q=%d c=%d r=%d f=%d", len(queued), len(completed), len(running), len(failed))
	}
}

func TestRecoveryWithSignalsPreserved(t *testing.T) {
	path := t.TempDir() + "/recovery-signals.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-sig-001", "t")

	for i := 0; i < 5; i++ {
		e1.SendSignal("rec-sig-001", "ping", map[string]int{"n": i})
	}
	e1.ConsumeSignal("rec-sig-001")

	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	all, _ := e2.GetSignals("rec-sig-001", false)
	if len(all) != 5 {
		t.Errorf("expected 5 signals, got %d", len(all))
	}

	pending, _ := e2.GetSignals("rec-sig-001", true)
	if len(pending) != 4 {
		t.Errorf("expected 4 pending signals (1 consumed), got %d", len(pending))
	}

	sig, _ := e2.ConsumeSignal("rec-sig-001")
	if sig == nil {
		t.Fatal("expected to consume a signal")
	}

	remaining, _ := e2.GetSignals("rec-sig-001", true)
	if len(remaining) != 3 {
		t.Errorf("expected 3 pending after second consume, got %d", len(remaining))
	}
}

func TestRecoveryWithTimersPreserved(t *testing.T) {
	path := t.TempDir() + "/recovery-timers.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-tmr-001", "t")
	e1.CreateTimer("rec-tmr-001", time.Now().Add(1*time.Hour))
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	pending, _ := e2.GetPendingTimers("rec-tmr-001")
	if len(pending) != 1 {
		t.Errorf("expected 1 pending timer, got %d", len(pending))
	}
	if pending[0].Fired {
		t.Error("timer should not be fired after recovery")
	}
}

func TestRecoveryWithAllEventTypes(t *testing.T) {
	path := t.TempDir() + "/recovery-events.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-ev-001", "t")
	wf, _ := e1.AcquireLease("w1", 10*time.Second)
	e1.StartStep(wf.WorkflowID, "w1", 1, nil)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e1.StartStep(wf.WorkflowID, "w1", 2, nil)
	e1.FailStep(wf.WorkflowID, "w1", 2, nil, errors.New("recovery error"))
	e1.CompleteStep(wf.WorkflowID, "w1", 2, map[string]int{}, map[string]string{"retried": "yes"})
	e1.CompleteWorkflow(wf.WorkflowID, "w1")

	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	events, _ := e2.GetEvents("rec-ev-001")
	if len(events) < 7 {
		t.Errorf("expected at least 7 events, got %d", len(events))
	}

	expectedTypes := []string{
		EventWorkflowCreated,
		EventWorkflowStarted,
		EventStepStarted,
		EventStepCompleted,
		EventStepStarted,
		EventStepFailed,
		EventStepCompleted,
		EventWorkflowCompleted,
	}

	for i, expected := range expectedTypes {
		if i >= len(events) {
			t.Errorf("missing event %d: expected %s", i, expected)
			break
		}
		if events[i].EventType != expected {
			t.Errorf("event %d: expected %s, got %s", i, expected, events[i].EventType)
		}
	}
}

func TestEngineCloseDoubleCall(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestEngineReopenAndReuse(t *testing.T) {
	path := t.TempDir() + "/reopen.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("reopen-001", "t")
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	wf, err := e2.GetWorkflow("reopen-001")
	if err != nil {
		t.Fatal(err)
	}
	if wf.WorkflowID != "reopen-001" {
		t.Errorf("expected reopen-001, got %s", wf.WorkflowID)
	}
}

func TestRecoveryThenFullWorkflowLifecycle(t *testing.T) {
	path := t.TempDir() + "/recovery-full.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-full-001", "t")
	wf, _ := e1.AcquireLease("w1", 0)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, map[string]string{"step": "1"}, map[string]string{"result": "ok1"})
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	s := NewScheduler(e2, 100*time.Millisecond)
	s.Tick()

	wf, err = e2.AcquireLease("w2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wf == nil {
		t.Fatal("expected to re-acquire workflow")
	}
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf.CurrentStep)
	}

	e2.CompleteStep(wf.WorkflowID, "w2", 2, map[string]string{"step": "2"}, map[string]string{"result": "ok2"})
	e2.CompleteWorkflow(wf.WorkflowID, "w2")

	wf, _ = e2.GetWorkflow("rec-full-001")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}

	steps, _ := e2.GetSteps("rec-full-001")
	if len(steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(steps))
	}
}

func TestRecoveryWithOpenTransaction(t *testing.T) {
	path := t.TempDir() + "/recovery-open-tx.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-tx-001", "t")
	wf, _ := e1.AcquireLease("w1", 10*time.Second)

	tx, _ := e1.db.Begin()
	if tx != nil {
		t.Log("begun transaction for simulation, closing engine without commit")
		tx.Rollback()
	}
	e1.Close()

	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	wf, err = e2.GetWorkflow("rec-tx-001")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusRunning {
		t.Errorf("expected RUNNING (transaction rolled back, no changes), got %s", wf.Status)
	}
}
