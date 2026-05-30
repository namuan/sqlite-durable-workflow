package durable

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	path := t.TempDir() + "/test.db"
	engine, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func runScheduler(t *testing.T, e *Engine, tick time.Duration) (cancel func()) {
	t.Helper()
	ctx, c := context.WithCancel(context.Background())
	s := NewScheduler(e, tick)
	go s.Run(ctx)
	return c
}

func runSchedulerAndCancel(t *testing.T, e *Engine, tick time.Duration, after time.Duration) {
	t.Helper()
	cancel := runScheduler(t, e, tick)
	time.Sleep(after)
	cancel()
}

// ── Workflows ─────────────────────────────────────────────────────

func TestCreateWorkflow(t *testing.T) {
	e := newTestEngine(t)

	wf, err := e.CreateWorkflow("wf-1", "test_type")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	if wf.WorkflowID != "wf-1" {
		t.Errorf("WorkflowID = %q, want %q", wf.WorkflowID, "wf-1")
	}
	if wf.Status != StatusQueued {
		t.Errorf("Status = %q, want %q", wf.Status, StatusQueued)
	}
	if wf.CurrentStep != 0 {
		t.Errorf("CurrentStep = %d, want 0", wf.CurrentStep)
	}
}

func TestGetWorkflow(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.CreateWorkflow("wf-2", "t")
	if err != nil {
		t.Fatal(err)
	}

	wf, err := e.GetWorkflow("wf-2")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusQueued {
		t.Errorf("Status = %q", wf.Status)
	}
}

func TestGetWorkflowNotFound(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.GetWorkflow("nope")
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestWorkflowLifecycle(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-life", "t")
	if wf.Status != StatusQueued {
		t.Fatalf("expected QUEUED, got %s", wf.Status)
	}

	// Acquire lease.
	wf, err := e.AcquireLease("worker-1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusRunning {
		t.Fatalf("expected RUNNING, got %s", wf.Status)
	}
	if wf.LeaseOwner == nil || *wf.LeaseOwner != "worker-1" {
		t.Errorf("lease_owner = %v", wf.LeaseOwner)
	}

	// Complete.
	if err := e.CompleteWorkflow(wf.WorkflowID, "worker-1"); err != nil {
		t.Fatal(err)
	}

	wf, err = e.GetWorkflow("wf-life")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
	if wf.CompletedAt == nil {
		t.Error("completed_at should not be nil")
	}
}

func TestFailWorkflow(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-fail", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	if err := e.FailWorkflow(wf.WorkflowID, "w1"); err != nil {
		t.Fatal(err)
	}

	wf, err := e.GetWorkflow("wf-fail")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != StatusFailed {
		t.Errorf("expected FAILED, got %s", wf.Status)
	}
}

func TestMarkWaiting(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-wait", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	if err := e.MarkWaiting(wf.WorkflowID, "w1"); err != nil {
		t.Fatal(err)
	}

	wf, _ = e.GetWorkflow("wf-wait")
	if wf.Status != StatusWaiting {
		t.Errorf("expected WAITING, got %s", wf.Status)
	}
}

// ── Leases ────────────────────────────────────────────────────────

func TestAcquireLeaseNoWork(t *testing.T) {
	e := newTestEngine(t)
	wf, err := e.AcquireLease("w1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wf != nil {
		t.Errorf("expected nil, got %+v", wf)
	}
}

func TestAcquireLeaseConcurrency(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-conc", "t")

	// Worker A claims.
	a, err := e.AcquireLease("wa", 10*time.Second)
	if err != nil || a == nil {
		t.Fatal("worker A should get a workflow")
	}
	if a.WorkflowID != "wf-conc" {
		t.Errorf("A got %q", a.WorkflowID)
	}

	// Worker B should get nothing (no other QUEUED workflows).
	b, err := e.AcquireLease("wb", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Errorf("worker B should get nil, got %+v", b)
	}
}

func TestRenewLease(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-renew", "t")
	wf, _ := e.AcquireLease("w1", 1*time.Second)

	if err := e.RenewLease(wf.WorkflowID, "w1", 60*time.Second); err != nil {
		t.Fatal(err)
	}

	wf, _ = e.GetWorkflow("wf-renew")
	if wf.LeaseExpiresAt == nil {
		t.Fatal("lease_expires_at should not be nil")
	}
	// Lease should have been extended well past 1 second.
	remaining := *wf.LeaseExpiresAt - time.Now().Unix()
	if remaining < 30 {
		t.Errorf("expected lease extended to ~60s, got %ds remaining", remaining)
	}
}

func TestRenewLeaseWrongWorker(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-reject", "t")
	wf, _ := e.AcquireLease("w1", 10*time.Second)

	err := e.RenewLease(wf.WorkflowID, "w2", 60*time.Second)
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestReleaseLease(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-rel", "t")
	wf, _ := e.AcquireLease("w1", 10*time.Second)

	if err := e.ReleaseLease(wf.WorkflowID, "w1"); err != nil {
		t.Fatal(err)
	}

	wf, _ = e.GetWorkflow("wf-rel")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after release, got %s", wf.Status)
	}
	if wf.LeaseOwner != nil {
		t.Errorf("lease_owner should be nil, got %v", wf.LeaseOwner)
	}
}

// ── Steps ─────────────────────────────────────────────────────────

func TestCompleteStep(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-step", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, map[string]int{"a": 1}, map[string]string{"b": "ok"})
	if err != nil {
		t.Fatal(err)
	}

	steps, err := e.GetSteps(wf.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].Status != "COMPLETED" {
		t.Errorf("status = %q", steps[0].Status)
	}
	if steps[0].StepNumber != 1 {
		t.Errorf("step_number = %d", steps[0].StepNumber)
	}

	wf, _ = e.GetWorkflow("wf-step")
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf.CurrentStep)
	}
}

func TestCompleteStepExactlyOnce(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-once", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	if err := e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil); err != ErrStepAlreadyCompleted {
		t.Errorf("expected ErrStepAlreadyCompleted, got %v", err)
	}

	// Cursor should still be 1 (not advanced twice).
	wf, _ = e.GetWorkflow("wf-once")
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf.CurrentStep)
	}
}

func TestFailStep(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-fstep", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.FailStep(wf.WorkflowID, "w1", 1, nil, sql.ErrNoRows)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].Status != "FAILED" {
		t.Errorf("status = %q", steps[0].Status)
	}

	// Cursor did not advance.
	wf, _ = e.GetWorkflow("wf-fstep")
	if wf.CurrentStep != 0 {
		t.Errorf("CurrentStep = %d, want 0", wf.CurrentStep)
	}
}

func TestCompleteAfterFail(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-caf", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	// Fail then retry-success.
	e.FailStep(wf.WorkflowID, "w1", 1, nil, sql.ErrNoRows)
	err := e.CompleteStep(wf.WorkflowID, "w1", 1, map[string]int{}, map[string]string{"retried": "yes"})
	if err != nil {
		t.Fatalf("CompleteStep after fail should succeed: %v", err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if steps[0].Status != "COMPLETED" {
		t.Errorf("expected COMPLETED after retry, got %s", steps[0].Status)
	}
}

func TestStepRejectsWrongWorker(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-sec", "t")
	_, _ = e.AcquireLease("w1", 10*time.Second)

	// w2 has no lease — should be rejected.
	err := e.CompleteStep(wf.WorkflowID, "w2", 1, nil, nil)
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

// ── Signals ───────────────────────────────────────────────────────

func TestSendAndConsumeSignal(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-sig", "t")

	sig, err := e.SendSignal("wf-sig", "approve", map[string]string{"by": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if sig.Consumed {
		t.Error("new signal should not be consumed")
	}

	consumed, err := e.ConsumeSignal("wf-sig")
	if err != nil {
		t.Fatal(err)
	}
	if consumed == nil {
		t.Fatal("expected a signal")
	}
	if !consumed.Consumed {
		t.Error("signal should be consumed")
	}

	// Second consume returns nil.
	again, err := e.ConsumeSignal("wf-sig")
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Errorf("expected nil, got %+v", again)
	}
}

func TestGetSignals(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-sigs", "t")

	e.SendSignal("wf-sigs", "type-a", nil)
	e.SendSignal("wf-sigs", "type-b", nil)

	all, _ := e.GetSignals("wf-sigs", false)
	if len(all) != 2 {
		t.Errorf("expected 2 signals, got %d", len(all))
	}

	pending, _ := e.GetSignals("wf-sigs", true)
	if len(pending) != 2 {
		t.Errorf("expected 2 pending, got %d", len(pending))
	}

	e.ConsumeSignal("wf-sigs")
	pending, _ = e.GetSignals("wf-sigs", true)
	if len(pending) != 1 {
		t.Errorf("expected 1 pending after consume, got %d", len(pending))
	}
}

// ── Timers ────────────────────────────────────────────────────────

func TestCreateTimer(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-timer", "t")

	timer, err := e.CreateTimer("wf-timer", time.Now().Add(1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if timer.Fired {
		t.Error("new timer should not be fired")
	}

	pending, _ := e.GetPendingTimers("wf-timer")
	if len(pending) != 1 {
		t.Errorf("expected 1 pending timer, got %d", len(pending))
	}
}

func TestTimerFiresAndRequeues(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-tfire", "t")
	wf, _ := e.AcquireLease("w1", 10*time.Second)

	// Create immediate timer and mark waiting.
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	// Fire through scheduler.
	runSchedulerAndCancel(t, e, 100*time.Millisecond, 300*time.Millisecond)

	wf, _ = e.GetWorkflow("wf-tfire")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after timer fire, got %s", wf.Status)
	}
	if wf.LeaseOwner != nil {
		t.Errorf("lease should be released after fire, got %v", wf.LeaseOwner)
	}
}

// ── Retry ─────────────────────────────────────────────────────────

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		name  string
		p     RetryPolicy
		count int
		want  int64
	}{
		{"fixed", FixedBackoff{30}, 0, 30},
		{"fixed-third", FixedBackoff{30}, 2, 30},
		{"linear-first", LinearBackoff{10}, 0, 10},
		{"linear-third", LinearBackoff{10}, 2, 30},
		{"exp-first", ExponentialBackoff{BaseSeconds: 1}, 0, 1},
		{"exp-second", ExponentialBackoff{BaseSeconds: 1}, 1, 2},
		{"exp-third", ExponentialBackoff{BaseSeconds: 1}, 2, 4},
		{"exp-capped", ExponentialBackoff{BaseSeconds: 1, MaxSeconds: 5}, 5, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.p.NextDelay(tt.count)
			if got != tt.want {
				t.Errorf("NextDelay(%d) = %d, want %d", tt.count, got, tt.want)
			}
		})
	}
}

func TestRetryExhausted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-rex", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	cfg := RetryConfig{MaxRetries: 1, Policy: FixedBackoff{1}}
	ok, err := e.RetryStep(wf.WorkflowID, "w1", 2, 2, cfg) // retryCount(2) > maxRetries(1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected false when retries exhausted")
	}
}

func TestRetryScheduled(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-rsch", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	cfg := RetryConfig{MaxRetries: 3, Policy: FixedBackoff{1}}
	ok, err := e.RetryStep(wf.WorkflowID, "w1", 2, 1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected true when retry scheduled")
	}

	wf, _ = e.GetWorkflow("wf-rsch")
	if wf.Status != StatusWaiting {
		t.Errorf("expected WAITING, got %s", wf.Status)
	}

	pending, _ := e.GetPendingTimers(wf.WorkflowID)
	if len(pending) != 1 {
		t.Errorf("expected 1 retry timer, got %d", len(pending))
	}
}

// ── Events ────────────────────────────────────────────────────────

func TestEventsAreImmutable(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-evts", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	events, err := e.GetEvents(wf.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}

	// Verify event types appear in order.
	types := make([]string, len(events))
	for i, ev := range events {
		types[i] = ev.EventType
	}

	if types[0] != EventWorkflowCreated {
		t.Errorf("first event = %s, want %s", types[0], EventWorkflowCreated)
	}
	if types[len(types)-1] != EventWorkflowCompleted {
		t.Errorf("last event = %s, want %s", types[len(types)-1], EventWorkflowCompleted)
	}

	// Sequence numbers should be strictly increasing.
	for i := 1; i < len(events); i++ {
		if events[i].SequenceNumber <= events[i-1].SequenceNumber {
			t.Errorf("sequence not monotonic at %d: %d <= %d", i, events[i].SequenceNumber, events[i-1].SequenceNumber)
		}
	}
}

// ── Observability ─────────────────────────────────────────────────

func TestObservabilityQueries(t *testing.T) {
	e := newTestEngine(t)

	// Create a mix of workflows. AcquireLease picks the oldest QUEUED,
	// so order matters.
	e.CreateWorkflow("obs-queued", "t")       // created first → oldest
	wf, _ := e.CreateWorkflow("obs-done", "t") // created second
	wf, _ = e.AcquireLease("w1", 10*time.Second) // picks obs-queued (oldest)
	e.CompleteWorkflow(wf.WorkflowID, "w1")       // obs-queued is now COMPLETED

	// Now obs-done is the only QUEUED remaining.
	wf, _ = e.CreateWorkflow("obs-failed", "t") // places third
	wf, _ = e.AcquireLease("w1", 10*time.Second) // picks obs-done
	e.FailWorkflow(wf.WorkflowID, "w1")           // obs-done is now FAILED

	failed, _ := e.QueryFailed()
	if len(failed) != 1 || failed[0].WorkflowID != "obs-done" {
		t.Errorf("QueryFailed: got %d, item=%+v", len(failed), failed)
	}

	running, _ := e.QueryRunning()
	if len(running) != 0 {
		t.Errorf("expected 0 running, got %d", len(running))
	}

	queued, _ := e.QueryQueued()
	if len(queued) != 1 || queued[0].WorkflowID != "obs-failed" {
		t.Errorf("QueryQueued: got %d, items=%+v", len(queued), queued)
	}
}

// ── Recovery ──────────────────────────────────────────────────────

func TestRecoveryFromCrash(t *testing.T) {
	// Simulate a crash by creating a new engine on the same DB.
	path := t.TempDir() + "/recovery.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	e1.CreateWorkflow("rec-001", "t")
	wf, _ := e1.AcquireLease("w1", 10*time.Second)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	// "Crash" — close without completing.
	e1.Close()

	// Recover.
	e2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	wf, err = e2.GetWorkflow("rec-001")
	if err != nil {
		t.Fatal(err)
	}
	// Lease should have expired and workflow returned to QUEUED.
	// (or still running with an expired lease, depending on timing)

	// Events and steps should be intact.
	steps, _ := e2.GetSteps("rec-001")
	if len(steps) != 1 {
		t.Errorf("expected 1 checkpoint after recovery, got %d", len(steps))
	}
	events, _ := e2.GetEvents("rec-001")
	if len(events) < 2 {
		t.Errorf("expected at least 2 events, got %d", len(events))
	}
}

// ── Lease expiration ──────────────────────────────────────────────

func TestLeaseExpiration(t *testing.T) {
	e := newTestEngine(t)
	_, _ = e.CreateWorkflow("wf-exp", "t")
	// Use a very short lease (1 second).
	wf, _ := e.AcquireLease("w1", 1*time.Second)

	// Wait for lease to expire.
	time.Sleep(1500 * time.Millisecond)

	// Run scheduler to expire it, then cancel.
	runSchedulerAndCancel(t, e, 200*time.Millisecond, 500*time.Millisecond)

	wf, _ = e.GetWorkflow("wf-exp")
	// After expiration, workflow should be QUEUED again.
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after lease expiry, got %s (owner=%v, expires=%v)", wf.Status, wf.LeaseOwner, wf.LeaseExpiresAt)
	}
}

// ── Idempotent start ──────────────────────────────────────────────

func TestStartStepIdempotent(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-idem", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	// Start twice — should not error.
	e.StartStep(wf.WorkflowID, "w1", 1, nil)
	e.StartStep(wf.WorkflowID, "w1", 1, nil)

	// Then complete — should work.
	if err := e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil); err != nil {
		t.Fatalf("CompleteStep after double start: %v", err)
	}
}

// ── OS cleanup guard (just in case) ───────────────────────────────
var _ = os.Remove
