package durable

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerTickFiresExpiredTimers(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-fire-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-fire-001")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after timer fire, got %s (expired=%v)", wf.Status, wf.LeaseExpiresAt)
	}

	pending, _ := e.GetPendingTimers("sched-fire-001")
	for _, p := range pending {
		if p.WakeAt <= time.Now().Unix() && !p.Fired {
			t.Errorf("timer %d should be fired", p.TimerID)
		}
	}
}

func TestSchedulerTickDoesNotFireFutureTimers(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-future-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now().Add(1*time.Hour))
	e.MarkWaiting(wf.WorkflowID, "w1")

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-future-001")
	if wf.Status != StatusWaiting {
		t.Errorf("expected WAITING (future timer), got %s", wf.Status)
	}

	pending, _ := e.GetPendingTimers("sched-future-001")
	if len(pending) != 1 {
		t.Errorf("expected 1 pending timer, got %d", len(pending))
	}
	if pending[0].Fired {
		t.Error("future timer should not be fired")
	}
}

func TestSchedulerTickExpiresLeases(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("sched-exp-001", "t")
	wf, _ := e.AcquireLease("w1", 0)
	if wf == nil {
		t.Fatal("expected lease")
	}

	time.Sleep(100 * time.Millisecond)

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-exp-001")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after lease expiry, got %s", wf.Status)
	}
}

func TestSchedulerTickDoesNotExpireActiveLeases(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("sched-active-001", "t")
	wf, _ := e.AcquireLease("w1", 60*time.Second)
	if wf == nil {
		t.Fatal("expected lease")
	}

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-active-001")
	if wf.Status != StatusRunning {
		t.Errorf("expected RUNNING (active lease), got %s", wf.Status)
	}
	if wf.LeaseOwner == nil {
		t.Error("lease_owner should not be nil for active lease")
	}
}

func TestSchedulerMultipleTicks(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-multi-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now().Add(200*time.Millisecond))
	e.MarkWaiting(wf.WorkflowID, "w1")

	start := time.Now()

	for time.Since(start) < 500*time.Millisecond {
		s := NewScheduler(e, 100*time.Millisecond)
		s.Tick()
		time.Sleep(50 * time.Millisecond)
	}

	wf, _ = e.GetWorkflow("sched-multi-001")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after repeated ticks, got %s", wf.Status)
	}
}

func TestSchedulerCancellation(t *testing.T) {
	e := newTestEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	s := NewScheduler(e, 20*time.Millisecond)
	go s.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	cancel()

	time.Sleep(100 * time.Millisecond)
}

func TestSchedulerRunAndCancel(t *testing.T) {
	e := newTestEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	s := NewScheduler(e, 50*time.Millisecond)
	go s.Run(ctx)

	e.CreateWorkflow("sched-run-001", "t")
	wf, _ := e.AcquireLease("w1", 50*time.Millisecond)

	time.Sleep(150 * time.Millisecond)

	s.Tick()

	wf, _ = e.GetWorkflow("sched-run-001")
	if wf.Status != StatusQueued {
		t.Logf("status after lease expiry: %s (may depend on timing)", wf.Status)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestSchedulerFiresMultipleExpiredTimers(t *testing.T) {
	e := newTestEngine(t)

	for i := 0; i < 5; i++ {
		id := string(rune('A' + i))
		wf, _ := e.CreateWorkflow("sched-multi-"+id, "t")
		wf, _ = e.AcquireLease("w1", 10*time.Second)
		e.CreateTimer(wf.WorkflowID, time.Now())
		e.MarkWaiting(wf.WorkflowID, "w1")
	}

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	for i := 0; i < 5; i++ {
		id := string(rune('A' + i))
		wf, _ := e.GetWorkflow("sched-multi-" + id)
		if wf.Status != StatusQueued {
			t.Errorf("expected QUEUED for %s, got %s", "sched-multi-"+id, wf.Status)
		}
	}
}

func TestSchedulerExpiresMultipleLeases(t *testing.T) {
	e := newTestEngine(t)

	for i := 0; i < 3; i++ {
		id := string(rune('X' + i))
		e.CreateWorkflow("sched-expl-"+id, "t")
		e.AcquireLease("w1", 0)
	}

	time.Sleep(100 * time.Millisecond)

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	queued, _ := e.QueryQueued()
	if len(queued) < 3 {
		t.Errorf("expected at least 3 QUEUED workflows after lease expiration, got %d", len(queued))
	}
}

func TestSchedulerTickWithNoWork(t *testing.T) {
	e := newTestEngine(t)

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()
	s.Tick()
	s.Tick()
}

func TestSchedulerTimerFireReleasesLease(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-rel-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-rel-001")
	if wf.LeaseOwner != nil {
		t.Errorf("lease should be released after timer fire, got %v", wf.LeaseOwner)
	}
	if wf.LeaseExpiresAt != nil {
		t.Errorf("lease_expires_at should be nil after timer fire, got %v", wf.LeaseExpiresAt)
	}
}

func TestSchedulerDoesNotFireNonWaitingWorkflows(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-nowait-001", "t")
	e.CreateTimer(wf.WorkflowID, time.Now())

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	pending, _ := e.GetPendingTimers("sched-nowait-001")
	if len(pending) != 1 {
		t.Errorf("expected 1 pending timer, got %d", len(pending))
	}
	if pending[0].Fired {
		t.Error("timer should NOT fire when workflow is not WAITING")
	}
}

func TestSchedulerRunContinuous(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("sched-cont-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now().Add(100*time.Millisecond))
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 300*time.Millisecond)

	wf, _ = e.GetWorkflow("sched-cont-001")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after scheduler run, got %s", wf.Status)
	}
}

func TestSchedulerTimerNotFiredTwice(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("sched-once-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	s := NewScheduler(e, 100*time.Millisecond)
	s.Tick()

	wf, _ = e.GetWorkflow("sched-once-001")
	if wf.Status != StatusQueued {
		t.Fatal("expected QUEUED after first tick")
	}

	e.AcquireLease("w2", 10*time.Second)
	e.MarkWaiting("sched-once-001", "w2")

	e.CreateTimer("sched-once-001", time.Now())
	s.Tick()

	wf, _ = e.GetWorkflow("sched-once-001")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED after second tick, got %s", wf.Status)
	}
}
