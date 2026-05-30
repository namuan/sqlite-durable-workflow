package durable

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestCreateWorkflowDefaults(t *testing.T) {
	e := newTestEngine(t)

	wf, err := e.CreateWorkflow("wf-defaults", "test_type")
	if err != nil {
		t.Fatal(err)
	}

	if wf.WorkflowID != "wf-defaults" {
		t.Errorf("WorkflowID = %q", wf.WorkflowID)
	}
	if wf.WorkflowType != "test_type" {
		t.Errorf("WorkflowType = %q", wf.WorkflowType)
	}
	if wf.Status != StatusQueued {
		t.Errorf("Status = %q, want %q", wf.Status, StatusQueued)
	}
	if wf.CurrentStep != 0 {
		t.Errorf("CurrentStep = %d, want 0", wf.CurrentStep)
	}
	if wf.LeaseOwner != nil {
		t.Errorf("LeaseOwner should be nil, got %v", wf.LeaseOwner)
	}
	if wf.LeaseExpiresAt != nil {
		t.Errorf("LeaseExpiresAt should be nil, got %v", wf.LeaseExpiresAt)
	}
	if wf.CreatedAt == 0 {
		t.Error("CreatedAt should not be zero")
	}
	if wf.UpdatedAt == 0 {
		t.Error("UpdatedAt should not be zero")
	}
	if wf.CompletedAt != nil {
		t.Errorf("CompletedAt should be nil, got %v", wf.CompletedAt)
	}
	if wf.Version == 0 {
		t.Error("Version should not be zero")
	}
}

func TestDuplicateWorkflowID(t *testing.T) {
	e := newTestEngine(t)

	_, err := e.CreateWorkflow("wf-dup", "type-a")
	if err != nil {
		t.Fatal(err)
	}

	_, err = e.CreateWorkflow("wf-dup", "type-b")
	if err == nil {
		t.Error("expected error on duplicate workflow ID")
	}
}

func TestAllStatusesReachable(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-states", "t")
	if wf.Status != StatusQueued {
		t.Fatalf("expected QUEUED, got %s", wf.Status)
	}

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	if wf.Status != StatusRunning {
		t.Fatalf("expected RUNNING, got %s", wf.Status)
	}

	e.MarkWaiting(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-states")
	if wf.Status != StatusWaiting {
		t.Fatalf("expected WAITING, got %s", wf.Status)
	}

	e.ReleaseLease(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-states")
	if wf.Status != StatusQueued {
		t.Fatalf("expected QUEUED after release, got %s", wf.Status)
	}

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-states")
	if wf.Status != StatusCompleted {
		t.Fatalf("expected COMPLETED, got %s", wf.Status)
	}
}

func TestAllStatusesReachableFailed(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-fail-state", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wf.WorkflowID, "w1")

	wf, _ = e.GetWorkflow("wf-fail-state")
	if wf.Status != StatusFailed {
		t.Errorf("expected FAILED, got %s", wf.Status)
	}
}

func TestCompleteWorkflowWithoutLease(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-no-lease", "t")

	err := e.CompleteWorkflow(wf.WorkflowID, "w1")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestFailWorkflowWithoutLease(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-fail-nolease", "t")

	err := e.FailWorkflow(wf.WorkflowID, "w1")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestMarkWaitingWithoutLease(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-mw-nolease", "t")

	err := e.MarkWaiting(wf.WorkflowID, "w1")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestReleaseLeaseWithoutOwning(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-rl-wrong", "t")
	_, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.ReleaseLease(wf.WorkflowID, "w2")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestCompleteWorkflowAlreadyCompleted(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-already-done", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	err := e.CompleteWorkflow(wf.WorkflowID, "w1")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch on already COMPLETED, got %v", err)
	}
}

func TestFailWorkflowAlreadyFailed(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-already-failed", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wf.WorkflowID, "w1")

	err := e.FailWorkflow(wf.WorkflowID, "w1")
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch on already FAILED, got %v", err)
	}
}

func TestTerminalStatesNotAcquired(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-c-done", "t")
	e.CreateWorkflow("wf-c-fail", "t")
	e.CreateWorkflow("wf-c-queued", "t")

	wf, _ := e.AcquireLease("w1", 10*time.Second)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wf.WorkflowID, "w1")

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	if wf == nil {
		t.Error("expected to acquire the QUEUED workflow")
	} else if wf.WorkflowID != "wf-c-queued" {
		t.Errorf("expected wf-c-queued, got %s", wf.WorkflowID)
	}

	next, _ := e.AcquireLease("w1", 10*time.Second)
	if next != nil {
		t.Errorf("expected nil after all eligible workflows consumed, got %s", next.WorkflowID)
	}
}

func TestAcquireLeaseSkipsRunning(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-held", "t")
	e.CreateWorkflow("wf-free", "t")

	wf, _ := e.AcquireLease("w1", 10*time.Second)

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected w2 to acquire another workflow")
	}
	if wf2.WorkflowID != "wf-free" {
		t.Errorf("expected wf-free, got %s (should skip held wf=%s)", wf2.WorkflowID, wf.WorkflowID)
	}
}

func TestAcquireLeaseSkipsFailed(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-failed-skip", "t")
	e.CreateWorkflow("wf-next", "t")

	wf, _ := e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wf.WorkflowID, "w1")

	wf2, _ := e.AcquireLease("w1", 10*time.Second)
	if wf2 == nil || wf2.WorkflowID != "wf-next" {
		t.Errorf("expected wf-next, got %+v", wf2)
	}
}

func TestVersionIncrementsOnAcquire(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-ver", "t")
	v1 := wf.Version

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	if wf.Version <= v1 {
		t.Errorf("expected version > %d after acquire, got %d", v1, wf.Version)
	}
}

func TestVersionIncrementsOnComplete(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-ver-comp", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	vBefore := wf.Version

	e.CompleteWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-ver-comp")
	if wf.Version <= vBefore {
		t.Errorf("expected version > %d after complete, got %d", vBefore, wf.Version)
	}
}

func TestVersionIncrementsOnFail(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-ver-fail", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	vBefore := wf.Version

	e.FailWorkflow(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-ver-fail")
	if wf.Version <= vBefore {
		t.Errorf("expected version > %d after fail, got %d", vBefore, wf.Version)
	}
}

func TestVersionIncrementsOnStepComplete(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-ver-step", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	vBefore := wf.Version

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	wf, _ = e.GetWorkflow("wf-ver-step")
	if wf.Version <= vBefore {
		t.Errorf("expected version > %d after step, got %d", vBefore, wf.Version)
	}
}

func TestVersionNotIncrementedOnMarkWaiting(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-ver-wait", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	vBefore := wf.Version

	e.MarkWaiting(wf.WorkflowID, "w1")
	wf, _ = e.GetWorkflow("wf-ver-wait")
	if wf.Version != vBefore {
		t.Errorf("expected version == %d after mark waiting, got %d", vBefore, wf.Version)
	}
}

func TestMarkWaitingPreservesLease(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-wait-lease", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.MarkWaiting(wf.WorkflowID, "w1")

	wf, _ = e.GetWorkflow("wf-wait-lease")
	if wf.LeaseOwner == nil || *wf.LeaseOwner != "w1" {
		t.Errorf("expected lease_owner=w1, got %v", wf.LeaseOwner)
	}
	if wf.LeaseExpiresAt == nil {
		t.Error("lease_expires_at should be preserved")
	}
}

func TestReleaseLeaseClearsLease(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-clear", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.ReleaseLease(wf.WorkflowID, "w1")

	wf, _ = e.GetWorkflow("wf-clear")
	if wf.Status != StatusQueued {
		t.Errorf("expected QUEUED, got %s", wf.Status)
	}
	if wf.LeaseOwner != nil {
		t.Errorf("lease_owner should be nil, got %v", wf.LeaseOwner)
	}
	if wf.LeaseExpiresAt != nil {
		t.Errorf("lease_expires_at should be nil, got %v", wf.LeaseExpiresAt)
	}
}

func TestAcquireLeaseOldestFirst(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-youngest", "t")
	time.Sleep(10 * time.Millisecond)
	e.CreateWorkflow("wf-oldest", "t")

	wf, _ := e.AcquireLease("w1", 10*time.Second)
	if wf == nil {
		t.Fatal("expected a workflow")
	}
	if wf.WorkflowID != "wf-youngest" {
		t.Errorf("expected oldest (wf-youngest), got %s", wf.WorkflowID)
	}
}

func TestAcquireLeaseReturnsWorkflowWithCorrectLease(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-correct", "t")
	wf, _ := e.AcquireLease("my-worker", 42*time.Second)

	if wf.LeaseOwner == nil || *wf.LeaseOwner != "my-worker" {
		t.Errorf("lease_owner = %v, want my-worker", wf.LeaseOwner)
	}
	if wf.LeaseExpiresAt == nil {
		t.Fatal("lease_expires_at is nil")
	}
	expectedExpiry := time.Now().Add(42 * time.Second).Unix()
	if diff := *wf.LeaseExpiresAt - expectedExpiry; diff > 15 || diff < -15 {
		t.Errorf("lease_expires_at within 5s of expected %d, got %d (diff=%d)", expectedExpiry, *wf.LeaseExpiresAt, diff)
	}
}

func TestAcquireLeaseWithExpiredTimer(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-exp-tmr", "t")
	wf, _ := e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	wf2, err := e.AcquireLease("w2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wf2 == nil {
		t.Error("expected to re-acquire because expired timer makes workflow eligible")
	}
	if wf2.WorkflowID != "wf-exp-tmr" {
		t.Errorf("expected wf-exp-tmr, got %s", wf2.WorkflowID)
	}
}

func TestAcquireLeaseWithExpiredTimerAfterScheduler(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-exp-tmr2", "t")
	wf, _ := e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 50*time.Millisecond, 200*time.Millisecond)

	wf2, _ := e.AcquireLease("w2", 10*time.Second)
	if wf2 == nil {
		t.Fatal("expected to re-acquire after timer fire")
	}
	if wf2.WorkflowID != "wf-exp-tmr2" {
		t.Errorf("expected wf-exp-tmr2, got %s", wf2.WorkflowID)
	}
}

func TestWorkflowIDsAreCaseSensitive(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("WF-Case", "t")
	e.CreateWorkflow("wf-case", "t")

	wf1, err := e.GetWorkflow("WF-Case")
	if err != nil {
		t.Fatal(err)
	}
	if wf1.WorkflowID != "WF-Case" {
		t.Errorf("expected WF-Case, got %s", wf1.WorkflowID)
	}

	wf2, err := e.GetWorkflow("wf-case")
	if err != nil {
		t.Fatal(err)
	}
	if wf2.WorkflowID != "wf-case" {
		t.Errorf("expected wf-case, got %s", wf2.WorkflowID)
	}
}

func TestCreateWorkflowIDsWithSpaces(t *testing.T) {
	e := newTestEngine(t)

	wf, err := e.CreateWorkflow("my workflow with spaces", "t")
	if err != nil {
		t.Fatal(err)
	}
	if wf.WorkflowID != "my workflow with spaces" {
		t.Errorf("WorkflowID = %q", wf.WorkflowID)
	}

	got, err := e.GetWorkflow("my workflow with spaces")
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowID != "my workflow with spaces" {
		t.Errorf("WorkflowID = %q", got.WorkflowID)
	}
}

func TestCreateWorkflowIDsWithSpecialChars(t *testing.T) {
	e := newTestEngine(t)

	id := "wf-!@#$%^&*()_+-=[]{}|;':\",./<>?"
	_, err := e.CreateWorkflow(id, "t")
	if err != nil {
		t.Fatal(err)
	}

	got, err := e.GetWorkflow(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowID != id {
		t.Errorf("expected %q, got %q", id, got.WorkflowID)
	}
}

func TestCreateWorkflowLongID(t *testing.T) {
	e := newTestEngine(t)

	longID := strings.Repeat("x", 1024)
	wf, err := e.CreateWorkflow(longID, "t")
	if err != nil {
		t.Fatal(err)
	}
	if wf.WorkflowID != longID {
		t.Errorf("WorkflowID mismatch")
	}
}

func TestCreateWorkflowLongType(t *testing.T) {
	e := newTestEngine(t)

	longType := strings.Repeat("y", 1024)
	wf, err := e.CreateWorkflow("wf-longtype", longType)
	if err != nil {
		t.Fatal(err)
	}
	if wf.WorkflowType != longType {
		t.Errorf("WorkflowType mismatch")
	}
}

func TestCreateWorkflowUnicodeID(t *testing.T) {
	e := newTestEngine(t)

	id := "wf-🚀-ワークフロー"
	wf, err := e.CreateWorkflow(id, "t")
	if err != nil {
		t.Fatal(err)
	}
	if wf.WorkflowID != id {
		t.Errorf("expected %q, got %q", id, wf.WorkflowID)
	}
}

func TestCompleteStepAdvancesCurrentStep(t *testing.T) {
	e := newTestEngine(t)

	wf, _ := e.CreateWorkflow("wf-adv", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	for i := 1; i <= 5; i++ {
		e.CompleteStep(wf.WorkflowID, "w1", i, nil, nil)
	}

	wf, _ = e.GetWorkflow("wf-adv")
	if wf.CurrentStep != 5 {
		t.Errorf("CurrentStep = %d, want 5", wf.CurrentStep)
	}
}

func TestWorkflowTypeIsPreserved(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-type", "order_processing")
	wf, _ := e.GetWorkflow("wf-type")
	if wf.WorkflowType != "order_processing" {
		t.Errorf("WorkflowType = %q", wf.WorkflowType)
	}

	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)

	wf, _ = e.GetWorkflow("wf-type")
	if wf.WorkflowType != "order_processing" {
		t.Errorf("WorkflowType changed after step: %q", wf.WorkflowType)
	}
}

func TestAcquireLeaseWithZeroDuration(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-zero", "t")
	wf, err := e.AcquireLease("w1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if wf == nil {
		t.Fatal("expected a workflow")
	}
	if wf.LeaseExpiresAt == nil {
		t.Fatal("lease_expires_at is nil")
	}
	now := time.Now().Unix()
	if *wf.LeaseExpiresAt < now-1 || *wf.LeaseExpiresAt > now+1 {
		t.Errorf("expected lease_expires_at near now (%d), got %d", now, *wf.LeaseExpiresAt)
	}
}

func TestAcquireLeaseWithVeryLongDuration(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-long", "t")
	duration := 365 * 24 * time.Hour
	wf, err := e.AcquireLease("w1", duration)
	if err != nil {
		t.Fatal(err)
	}
	if wf == nil || wf.LeaseExpiresAt == nil {
		t.Fatal("expected lease")
	}

	now := time.Now().Unix()
	expected := now + int64(duration.Seconds())
	if *wf.LeaseExpiresAt < expected-5 || *wf.LeaseExpiresAt > expected+5 {
		t.Errorf("lease_expires_at out of range")
	}
}

func TestAcquireLeaseWithMaxIntDuration(t *testing.T) {
	e := newTestEngine(t)

	e.CreateWorkflow("wf-max", "t")
	wf, err := e.AcquireLease("w1", time.Duration(math.MaxInt64))
	if err != nil {
		t.Fatal(err)
	}
	if wf == nil || wf.LeaseExpiresAt == nil {
		t.Fatal("expected lease")
	}
	if *wf.LeaseExpiresAt <= 0 {
		t.Errorf("invalid lease expiry: %d", *wf.LeaseExpiresAt)
	}
}
