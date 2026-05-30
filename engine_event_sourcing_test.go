package durable

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"
)

func TestEventsSequenceContinuity(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-seq-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	events, err := e.GetEvents(wf.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(events); i++ {
		if events[i].SequenceNumber != i+1 {
			t.Errorf("event %d: expected sequence %d, got %d", i, i+1, events[i].SequenceNumber)
		}
	}
}

func TestEventsWorkflowCreated(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.CreateWorkflow("ev-cr-001", "test_create")
	if err != nil {
		t.Fatal(err)
	}

	events, _ := e.GetEvents("ev-cr-001")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventType != EventWorkflowCreated {
		t.Errorf("expected %s, got %s", EventWorkflowCreated, events[0].EventType)
	}
	if events[0].SequenceNumber != 1 {
		t.Errorf("expected sequence 1, got %d", events[0].SequenceNumber)
	}
	if events[0].CreatedAt == 0 {
		t.Error("created_at should not be 0")
	}
}

func TestEventsWorkflowStarted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-st-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	events, _ := e.GetEvents(wf.WorkflowID)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[1].EventType != EventWorkflowStarted {
		t.Errorf("expected %s, got %s", EventWorkflowStarted, events[1].EventType)
	}
}

func TestEventsStepStarted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-ss-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.StartStep(wf.WorkflowID, "w1", 1, map[string]int{"n": 42})

	events, _ := e.GetEvents(wf.WorkflowID)
	found := false
	for _, ev := range events {
		if ev.EventType == EventStepStarted {
			found = true
			if ev.PayloadJSON != nil {
				var p map[string]int
				json.Unmarshal([]byte(*ev.PayloadJSON), &p)
				if p["n"] != 42 {
					t.Errorf("payload n = %d, want 42", p["n"])
				}
			}
			break
		}
	}
	if !found {
		t.Errorf("expected %s event not found", EventStepStarted)
	}
}

func TestEventsStepCompleted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-sc-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, map[string]string{"result": "done"})

	events, _ := e.GetEvents(wf.WorkflowID)
	found := false
	for _, ev := range events {
		if ev.EventType == EventStepCompleted {
			found = true
			if ev.PayloadJSON == nil {
				t.Error("expected payload_json for STEP_COMPLETED")
			} else {
				var p map[string]interface{}
				json.Unmarshal([]byte(*ev.PayloadJSON), &p)
				if int(p["step_number"].(float64)) != 1 {
					t.Errorf("step_number = %v", p["step_number"])
				}
			}
			break
		}
	}
	if !found {
		t.Errorf("expected %s event not found", EventStepCompleted)
	}
}

func TestEventsStepFailed(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-sf-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailStep(wf.WorkflowID, "w1", 1, nil, errors.New("step failed"))

	events, _ := e.GetEvents(wf.WorkflowID)
	found := false
	for _, ev := range events {
		if ev.EventType == EventStepFailed {
			found = true
			if ev.PayloadJSON != nil {
				var p map[string]interface{}
				json.Unmarshal([]byte(*ev.PayloadJSON), &p)
				if p["error"] == nil {
					t.Error("expected error in payload")
				}
			}
			break
		}
	}
	if !found {
		t.Errorf("expected %s event not found", EventStepFailed)
	}
}

func TestEventsSignalReceived(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("ev-sig-001", "t")
	e.SendSignal("ev-sig-001", "approve", map[string]string{"by": "alice"})

	events, _ := e.GetEvents("ev-sig-001")
	found := false
	for _, ev := range events {
		if ev.EventType == EventSignalReceived {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %s event not found", EventSignalReceived)
	}
}

func TestEventsTimerFired(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-tmr-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CreateTimer(wf.WorkflowID, time.Now())
	e.MarkWaiting(wf.WorkflowID, "w1")

	runSchedulerAndCancel(t, e, 100*time.Millisecond, 300*time.Millisecond)

	events, _ := e.GetEvents(wf.WorkflowID)
	found := false
	for _, ev := range events {
		if ev.EventType == EventTimerFired {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %s event not found", EventTimerFired)
	}
}

func TestEventsWorkflowCompleted(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-wc-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	events, _ := e.GetEvents(wf.WorkflowID)
	if events[len(events)-1].EventType != EventWorkflowCompleted {
		t.Errorf("expected %s as last event, got %s", EventWorkflowCompleted, events[len(events)-1].EventType)
	}
}

func TestEventsWorkflowFailed(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-wf-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wf.WorkflowID, "w1")

	events, _ := e.GetEvents(wf.WorkflowID)
	if events[len(events)-1].EventType != EventWorkflowFailed {
		t.Errorf("expected %s as last event, got %s", EventWorkflowFailed, events[len(events)-1].EventType)
	}
}

func TestEventsMultipleWorkflowsIsolated(t *testing.T) {
	e := newTestEngine(t)

	wfA, _ := e.CreateWorkflow("ev-iso-a", "t")
	wfB, _ := e.CreateWorkflow("ev-iso-b", "t")

	wfA, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wfA.WorkflowID, "w1", 1, nil, nil)
	e.CompleteWorkflow(wfA.WorkflowID, "w1")

	wfB, _ = e.AcquireLease("w1", 10*time.Second)
	e.FailWorkflow(wfB.WorkflowID, "w1")

	eventsA, _ := e.GetEvents("ev-iso-a")
	eventsB, _ := e.GetEvents("ev-iso-b")

	allIDsA := make(map[string]bool)
	for _, ev := range eventsA {
		if ev.WorkflowID != "ev-iso-a" {
			t.Errorf("event %d has workflow_id=%q in A's events", ev.SequenceNumber, ev.WorkflowID)
		}
		allIDsA[ev.WorkflowID] = true
	}
	if len(allIDsA) != 1 {
		t.Errorf("events A contain %d distinct workflow IDs, expected 1", len(allIDsA))
	}

	for _, ev := range eventsB {
		if ev.WorkflowID != "ev-iso-b" {
			t.Errorf("event %d has workflow_id=%q in B's events", ev.SequenceNumber, ev.WorkflowID)
		}
	}
}

func TestEventsSequenceMonotonic(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-mono-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	for i := 1; i <= 20; i++ {
		e.CompleteStep(wf.WorkflowID, "w1", i, nil, nil)
	}

	events, _ := e.GetEvents(wf.WorkflowID)
	for i := 1; i < len(events); i++ {
		if events[i].SequenceNumber <= events[i-1].SequenceNumber {
			t.Errorf("sequence not monotonic at %d: %d <= %d", i, events[i].SequenceNumber, events[i-1].SequenceNumber)
		}
	}
	if events[len(events)-1].SequenceNumber != len(events) {
		t.Errorf("max sequence = %d, want %d (no gaps)", events[len(events)-1].SequenceNumber, len(events))
	}
}

func TestEventsCreatedAtMonotonic(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-time-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	events, _ := e.GetEvents(wf.WorkflowID)
	for i := 1; i < len(events); i++ {
		if events[i].CreatedAt < events[i-1].CreatedAt {
			t.Errorf("created_at not monotonic at %d: %d < %d", i, events[i].CreatedAt, events[i-1].CreatedAt)
		}
	}
}

func TestReplayStateFromEvents(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-replay-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.FailStep(wf.WorkflowID, "w1", 2, nil, errors.New("step 2 failed"))
	e.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 3, nil, nil)
	e.CompleteWorkflow(wf.WorkflowID, "w1")

	events, _ := e.GetEvents(wf.WorkflowID)

	type replayedState struct {
		status    string
		cursor    int
		isRunning bool
	}

	state := replayedState{status: StatusQueued}

	for _, ev := range events {
		switch ev.EventType {
		case EventWorkflowCreated:
			state.status = StatusQueued
		case EventWorkflowStarted:
			state.status = StatusRunning
			state.isRunning = true
		case EventStepCompleted:
			state.cursor++
		case EventStepFailed:
		case EventWorkflowCompleted:
			state.status = StatusCompleted
			state.isRunning = false
		case EventWorkflowFailed:
			state.status = StatusFailed
			state.isRunning = false
		}
	}

	if state.status != StatusCompleted {
		t.Errorf("replayed status = %s, want %s", state.status, StatusCompleted)
	}
	if state.cursor != 3 {
		t.Errorf("replayed cursor = %d, want 3", state.cursor)
	}
}

func TestEventsPayloadCorrectnessForRetry(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("ev-retry-001", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.FailStep(wf.WorkflowID, "w1", 1, nil, errors.New("step failed"))
	e.CompleteStep(wf.WorkflowID, "w1", 1, map[string]int{"attempt": 2}, map[string]string{"result": "ok"})

	events, _ := e.GetEvents(wf.WorkflowID)

	var failPayload, completePayload string
	for _, ev := range events {
		if ev.EventType == EventStepFailed && ev.PayloadJSON != nil {
			failPayload = *ev.PayloadJSON
		}
		if ev.EventType == EventStepCompleted && ev.PayloadJSON != nil {
			completePayload = *ev.PayloadJSON
		}
	}

	var fp map[string]interface{}
	json.Unmarshal([]byte(failPayload), &fp)
	if fp["error"] == nil {
		t.Error("expected error in FAIL event payload")
	}

	var cp map[string]interface{}
	json.Unmarshal([]byte(completePayload), &cp)
	if cp["step_number"] == nil {
		t.Error("expected step_number in COMPLETE event payload")
	}
}

func TestEventsFromMultipleWorkflowsAreChronological(t *testing.T) {
	e := newTestEngine(t)

	wfA, _ := e.CreateWorkflow("ev-chrono-a", "t")
	wfA, _ = e.AcquireLease("w1", 10*time.Second)
	wfB, _ := e.CreateWorkflow("ev-chrono-b", "t")
	e.CompleteStep(wfA.WorkflowID, "w1", 1, nil, nil)
	wfB, _ = e.AcquireLease("w2", 10*time.Second)
	e.CompleteStep(wfB.WorkflowID, "w2", 1, nil, nil)

	eventsA, _ := e.GetEvents("ev-chrono-a")
	eventsB, _ := e.GetEvents("ev-chrono-b")

	for _, ev := range eventsA {
		if ev.EventID <= 0 {
			t.Errorf("event_id should be positive, got %d", ev.EventID)
		}
	}

	idsA := make([]int64, len(eventsA))
	idsB := make([]int64, len(eventsB))
	for i, ev := range eventsA {
		idsA[i] = ev.EventID
	}
	for i, ev := range eventsB {
		idsB[i] = ev.EventID
	}

	sort.Slice(idsA, func(i, j int) bool { return idsA[i] < idsA[j] })
	sort.Slice(idsB, func(i, j int) bool { return idsB[i] < idsB[j] })

	for i := 1; i < len(idsA); i++ {
		if idsA[i] <= idsA[i-1] {
			t.Errorf("event_id not monotonic within A: %d <= %d", idsA[i], idsA[i-1])
		}
	}
	for i := 1; i < len(idsB); i++ {
		if idsB[i] <= idsB[i-1] {
			t.Errorf("event_id not monotonic within B: %d <= %d", idsB[i], idsB[i-1])
		}
	}
}
