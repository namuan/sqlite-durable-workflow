package durable

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestComplexFullWorkflowLifecycle(t *testing.T) {
	e := newTestEngine(t)

	wf, err := e.CreateWorkflow("cplx-001", "order_saga")
	if err != nil {
		t.Fatal(err)
	}

	wf, err = e.AcquireLease("worker-a", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	e.StartStep(wf.WorkflowID, "worker-a", 1, map[string]string{"action": "validate"})
	e.CompleteStep(wf.WorkflowID, "worker-a", 1, map[string]string{"action": "validate"}, map[string]string{"valid": "true"})

	e.StartStep(wf.WorkflowID, "worker-a", 2, map[string]string{"action": "charge"})
	e.CompleteStep(wf.WorkflowID, "worker-a", 2, map[string]string{"action": "charge"}, map[string]string{"charged": "99.99"})

	e.StartStep(wf.WorkflowID, "worker-a", 3, map[string]string{"action": "ship"})
	e.FailStep(wf.WorkflowID, "worker-a", 3, map[string]string{"action": "ship"}, sql.ErrNoRows)

	cfg := RetryConfig{MaxRetries: 2, Policy: FixedBackoff{0}}
	ok, _ := e.RetryStep(wf.WorkflowID, "worker-a", 3, 1, cfg)
	if !ok {
		t.Fatal("expected retry to be scheduled")
	}

	runSchedulerAndCancel(t, e, 100*time.Millisecond, 500*time.Millisecond)

	wf2, _ := e.AcquireLease("worker-b", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected re-acquire after retry timer")
	}

	e.StartStep(wf2.WorkflowID, "worker-b", 3, map[string]string{"action": "ship", "attempt": "2"})
	e.CompleteStep(wf2.WorkflowID, "worker-b", 3, map[string]string{"action": "ship", "attempt": "2"}, map[string]string{"tracking": "1Z999"})

	e.CompleteWorkflow(wf2.WorkflowID, "worker-b")

	wf, _ = e.GetWorkflow("cplx-001")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
	if wf.CurrentStep != 3 {
		t.Errorf("CurrentStep = %d, want 3", wf.CurrentStep)
	}

	steps, _ := e.GetSteps("cplx-001")
	if len(steps) != 3 {
		t.Errorf("expected 3 steps, got %d", len(steps))
	}

	events, _ := e.GetEvents("cplx-001")
	if len(events) == 0 {
		t.Error("expected events")
	}
}

func TestComplexWorkflowWithRetryExhausted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-exh", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.FailStep(wf.WorkflowID, "w1", 1, nil, sql.ErrNoRows)
	e.FailStep(wf.WorkflowID, "w1", 1, nil, sql.ErrNoRows)

	cfg := RetryConfig{MaxRetries: 1, Policy: FixedBackoff{1}}
	ok, _ := e.RetryStep(wf.WorkflowID, "w1", 1, 2, cfg)
	if ok {
		t.Error("expected retry exhausted (retryCount=2 > maxRetries=1)")
	}

	e.FailWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("cplx-exh")
	if wf.Status != StatusFailed {
		t.Errorf("expected FAILED, got %s", wf.Status)
	}
}

func TestComplexWorkflowWithSignalAsExternalTrigger(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("cplx-sig-trig", "t")

	e.SendSignal("cplx-sig-trig", "webhook", map[string]interface{}{
		"event":   "payment.received",
		"order_id": "ORD-999",
	})

	e.SendSignal("cplx-sig-trig", "webhook", map[string]interface{}{
		"event":   "payment.confirmed",
		"order_id": "ORD-999",
	})

	sig, _ := e.ConsumeSignal("cplx-sig-trig")
	if sig == nil {
		t.Fatal("expected first signal")
	}

	wf, _ := e.AcquireLease("w1", 10*time.Second)
	e.StartStep(wf.WorkflowID, "w1", 1, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	sig2, _ := e.ConsumeSignal("cplx-sig-trig")
	if sig2 == nil {
		t.Fatal("expected second signal")
	}

	e.CompleteWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("cplx-sig-trig")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
}

func TestComplexWorkflowWithTimerFiresAndContinues(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-tmr-cont", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 200*time.Millisecond)

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected re-acquire after timer fire")
	}
	if wf2.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf2.CurrentStep)
	}

	e.CompleteStep(wf2.WorkflowID, "w2", 2, nil, nil)
	e.CompleteWorkflow(wf2.WorkflowID, "w2")

	wf, _ = e.GetWorkflow("cplx-tmr-cont")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
	if wf.CurrentStep != 2 {
		t.Errorf("CurrentStep = %d, want 2", wf.CurrentStep)
	}
}

func TestComplexMultipleTimersSingleWorkflow(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-multi-tmr", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	e.CreateTimer(wf.WorkflowID, time.Now().Add(50*time.Millisecond))
	e.CreateTimer(wf.WorkflowID, time.Now().Add(100*time.Millisecond))
	e.CreateTimer(wf.WorkflowID, time.Now().Add(150*time.Millisecond))
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 300*time.Millisecond)

	wf, _ = e.GetWorkflow("cplx-multi-tmr")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after timers fire, got %s", wf.Status)
	}
}

func TestComplexRecoverFromCrashAndComplete(t *testing.T) {
	path := t.TempDir() + "/cplx-recover.db"

	e1, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	wf, _ := e1.CreateWorkflow("cplx-recover-001", "critical_job")
	wf, _ = e1.AcquireLease("w1", 0)
	e1.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e1.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)

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
		t.Fatal("expected re-acquire")
	}
	if wf.CurrentStep != 2 {
		t.Errorf("CurrentStep = %d, want 2", wf.CurrentStep)
	}

	runSchedulerAndCancel(t, e2, 100*time.Millisecond, 300*time.Millisecond)

	e2.CompleteStep(wf.WorkflowID, "w2", 3, nil, nil)
	e2.CompleteWorkflow(wf.WorkflowID, "w2")

	wf, _ = e2.GetWorkflow("cplx-recover-001")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
}

func TestComplexInterleavedWorkflowProcessing(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("cplx-inter-a", "t")
	e.CreateWorkflow("cplx-inter-b", "t")

	wfA, _ := e.AcquireLease("w1", 10*time.Second)
	wfB, _ := e.AcquireLease("w2", 10*time.Second)

	steps := []struct {
		wf       *Workflow
		workerID string
		stepNum  int
	}{
		{wfA, "w1", 1},
		{wfB, "w2", 1},
		{wfA, "w1", 2},
		{wfB, "w2", 2},
		{wfA, "w1", 3},
		{wfB, "w2", 3},
	}

	for _, s := range steps {
		err := e.CompleteStep(s.wf.WorkflowID, s.workerID, s.stepNum, nil, nil)
		if err != nil {
			t.Fatalf("CompleteStep(%s, step %d): %v", s.wf.WorkflowID, s.stepNum, err)
		}
	}

	e.CompleteWorkflow(wfA.WorkflowID, "w1")
	e.CompleteWorkflow(wfB.WorkflowID, "w2")

	wfA, _ = e.GetWorkflow("cplx-inter-a")
	wfB, _ = e.GetWorkflow("cplx-inter-b")

	if wfA.Status != StatusCompleted {
		t.Errorf("A: expected COMPLETED, got %s", wfA.Status)
	}
	if wfA.CurrentStep != 3 {
		t.Errorf("A: CurrentStep = %d, want 3", wfA.CurrentStep)
	}
	if wfB.Status != StatusCompleted {
		t.Errorf("B: expected COMPLETED, got %s", wfB.Status)
	}
	if wfB.CurrentStep != 3 {
		t.Errorf("B: CurrentStep = %d, want 3", wfB.CurrentStep)
	}
}

func TestComplexWorkflowWithManySteps(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-many", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	numSteps := 50
	for i := 1; i <= numSteps; i++ {
		input := map[string]int{"step": i}
		output := map[string]int{"result": i * 2}
		err := e.CompleteStep(wf.WorkflowID, "w1", i, input, output)
		if err != nil {
			t.Fatalf("CompleteStep(%d): %v", i, err)
		}
	}

	wf, _ = e.GetWorkflow("cplx-many")
	if wf.CurrentStep != numSteps {
		t.Errorf("CurrentStep = %d, want %d", wf.CurrentStep, numSteps)
	}

	steps, _ := e.GetSteps("cplx-many")
	if len(steps) != numSteps {
		t.Errorf("expected %d steps, got %d", numSteps, len(steps))
	}

	e.CompleteWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("cplx-many")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
}

func TestComplexWorkflowSignalAndTimerInteraction(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-sig-tmr", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	e.SendSignal("cplx-sig-tmr", "notify", nil)

	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 200*time.Millisecond)

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected re-acquire after timer+scheduler")
	}

	sig, _ := e.ConsumeSignal("cplx-sig-tmr")
	if sig == nil {
		t.Error("expected signal to still be available after timer fire")
	}
	if sig.SignalType != "notify" {
		t.Errorf("expected 'notify', got %s", sig.SignalType)
	}

	e.CompleteStep(wf2.WorkflowID, "w2", 2, nil, nil)
	e.CompleteWorkflow(wf2.WorkflowID, "w2")
}

func TestComplexWorkflowChainedTimers(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-chain-tmr", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 200*time.Millisecond)

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected re-acquire after first timer")
	}
	e.CompleteStep(wf2.WorkflowID, "w2", 2, nil, nil)

	e.CreateTimer(wf2.WorkflowID, time.Now())
	e.MarkWaiting(wf2.WorkflowID, "w2")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 200*time.Millisecond)

	wf3, _ := e.AcquireLease("w3", 10*time.Second)
	if wf3 == nil {
		t.Fatal("expected re-acquire after second timer")
	}
	e.CompleteStep(wf3.WorkflowID, "w3", 3, nil, nil)
	e.CompleteWorkflow(wf3.WorkflowID, "w3")

	wf, _ = e.GetWorkflow("cplx-chain-tmr")
	if wf.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wf.Status)
	}
	if wf.CurrentStep != 3 {
		t.Errorf("CurrentStep = %d, want 3", wf.CurrentStep)
	}
}

func TestComplexWorkflowSlowStep(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-slow", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.StartStep(wf.WorkflowID, "w1", 1, map[string]string{"action": "slow_op"})

	time.Sleep(200 * time.Millisecond)

	e.CompleteStep(wf.WorkflowID, "w1", 1, map[string]string{"action": "slow_op"}, map[string]string{"done": "finally"})

	steps, _ := e.GetSteps("cplx-slow")
	if steps[0].StartedAt == nil || steps[0].CompletedAt == nil {
		t.Error("expected started_at and completed_at")
	}
	if steps[0].StartedAt != nil && steps[0].CompletedAt != nil {
		duration := *steps[0].CompletedAt - *steps[0].StartedAt
		if duration < 0 {
			t.Errorf("expected duration >= 0, got %dms", duration)
		}
	}
}

func TestComplexWorkflowParallelSignals(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("cplx-par-sig", "t")

	signalCount := 30
	for i := 0; i < signalCount; i++ {
		e.SendSignal("cplx-par-sig", fmt.Sprintf("sig-%d", i), nil)
	}

	consumed := 0
	for {
		sig, err := e.ConsumeSignal("cplx-par-sig")
		if err != nil {
			t.Fatal(err)
		}
		if sig == nil {
			break
		}
		consumed++
	}

	if consumed != signalCount {
		t.Errorf("expected to consume %d signals, got %d", signalCount, consumed)
	}
}

func TestComplexWorkflowLeaseTransfer(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("cplx-transfer", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.ReleaseLease(wf.WorkflowID, "w1")

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil || wf2.WorkflowID != "cplx-transfer" {
		t.Fatal("expected w2 to acquire released workflow")
	}
	if wf2.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf2.CurrentStep)
	}

	e.CompleteStep(wf2.WorkflowID, "w2", 2, nil, nil)
	e.CompleteWorkflow(wf2.WorkflowID, "w2")
}
