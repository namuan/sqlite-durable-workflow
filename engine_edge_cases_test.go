package durable

import (
	"database/sql"
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestNonexistentWorkflowOperations(t *testing.T) {
	e := newTestEngine(t)

	t.Run("SendSignal", func(t *testing.T) {
		_, err := e.SendSignal("does-not-exist", "ping", nil)
		if err == nil {
			t.Error("expected foreign key error when sending signal to nonexistent workflow")
		}
	})

	t.Run("CreateTimer", func(t *testing.T) {
		_, err := e.CreateTimer("does-not-exist", time.Now())
		if err == nil {
			t.Error("expected foreign key error when creating timer on nonexistent workflow")
		}
	})

	t.Run("GetEvents", func(t *testing.T) {
		events, err := e.GetEvents("does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 0 {
			t.Errorf("expected 0 events, got %d", len(events))
		}
	})

	t.Run("GetSteps", func(t *testing.T) {
		steps, err := e.GetSteps("does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if len(steps) != 0 {
			t.Errorf("expected 0 steps, got %d", len(steps))
		}
	})

	t.Run("GetSignals", func(t *testing.T) {
		signals, err := e.GetSignals("does-not-exist", false)
		if err != nil {
			t.Fatal(err)
		}
		if len(signals) != 0 {
			t.Errorf("expected 0 signals, got %d", len(signals))
		}
	})

	t.Run("GetPendingTimers", func(t *testing.T) {
		timers, err := e.GetPendingTimers("does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if len(timers) != 0 {
			t.Errorf("expected 0 timers, got %d", len(timers))
		}
	})

	t.Run("ConsumeSignal", func(t *testing.T) {
		sig, err := e.ConsumeSignal("does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if sig != nil {
			t.Errorf("expected nil signal, got %+v", sig)
		}
	})

	t.Run("RetryStep", func(t *testing.T) {
		cfg := RetryConfig{MaxRetries: 3, Policy: FixedBackoff{1}}
		_, err := e.RetryStep("does-not-exist", "w1", 1, 0, cfg)
		if err == nil {
			t.Error("expected foreign key error when retrying nonexistent workflow")
		}
	})
}

func TestNilInputPayload(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-nil-in", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, nil, map[string]string{"ok": "true"})
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 {
		t.Fatal("expected 1 step")
	}
	if steps[0].InputJSON != nil {
		t.Errorf("expected nil input_json, got %v", *steps[0].InputJSON)
	}
}

func TestNilOutputPayload(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-nil-out", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, map[string]int{"x": 1}, nil)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 {
		t.Fatal("expected 1 step")
	}
	if steps[0].OutputJSON != nil {
		t.Errorf("expected nil output_json, got %v", *steps[0].OutputJSON)
	}
}

func TestBothNilPayloads(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-both-nil", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if steps[0].Status != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", steps[0].Status)
	}
	wf, _ = e.GetWorkflow("wf-both-nil")
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf.CurrentStep)
	}
}

func TestVeryLargeStepNumber(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-bigstep", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	bigStep := math.MaxInt32
	err := e.CompleteStep(wf.WorkflowID, "w1", bigStep, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 || steps[0].StepNumber != bigStep {
		t.Errorf("expected step_number=%d, got %+v", bigStep, steps)
	}
}

func TestStepNumberZero(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-step0", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w1", 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	wf, _ = e.GetWorkflow("wf-step0")
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", wf.CurrentStep)
	}
}

func TestCompleteStepSkippingNumbers(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-gap", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 4, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 7, nil, nil)

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(steps))
	}
	nums := make([]int, len(steps))
	for i, s := range steps {
		nums[i] = s.StepNumber
	}
	if nums[0] != 4 || nums[1] != 7 {
		t.Errorf("expected [4, 7], got %v", nums)
	}
}

func TestCompleteStepOutOfOrder(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-ooo", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 5, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(steps))
	}

	wf, _ = e.GetWorkflow("wf-ooo")
	if wf.CurrentStep != 2 {
		t.Errorf("CurrentStep = %d, expected 2 (cursor advances twice)", wf.CurrentStep)
	}
}

func TestFailStartStepWithoutLease(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-start-nolease", "t")

	err := e.StartStep("wf-start-nolease", "w1", 1, nil)
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestFailCompleteStepWithoutLease(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-comp-nolease", "t")
	_, _ = e.AcquireLease("w1", 10*time.Second)

	err := e.CompleteStep(wf.WorkflowID, "w2", 1, nil, nil)
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestFailFailStepWithoutLease(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-ff-nolease", "t")

	err := e.FailStep("wf-ff-nolease", "w1", 1, nil, sql.ErrNoRows)
	if err != ErrLeaseMismatch {
		t.Errorf("expected ErrLeaseMismatch, got %v", err)
	}
}

func TestNestedJSONPayload(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-nested", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	type nested struct {
		User struct {
			Name  string   `json:"name"`
			Roles []string `json:"roles"`
		} `json:"user"`
		Metadata map[string]interface{} `json:"metadata"`
	}

	input := nested{
		Metadata: map[string]interface{}{
			"region": "us-east-1",
			"count":  42,
		},
	}
	input.User.Name = "alice"
	input.User.Roles = []string{"admin", "editor"}

	output := map[string]interface{}{
		"result": "success",
		"items":  []int{1, 2, 3},
	}

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, input, output)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if steps[0].InputJSON == nil || steps[0].OutputJSON == nil {
		t.Error("expected non-nil JSON payloads")
	}

	var decodedInput nested
	if err := json.Unmarshal([]byte(*steps[0].InputJSON), &decodedInput); err != nil {
		t.Errorf("failed to decode input JSON: %v", err)
	}
	if decodedInput.User.Name != "alice" {
		t.Errorf("User.Name = %q", decodedInput.User.Name)
	}

	var decodedOutput map[string]interface{}
	if err := json.Unmarshal([]byte(*steps[0].OutputJSON), &decodedOutput); err != nil {
		t.Errorf("failed to decode output JSON: %v", err)
	}
	if decodedOutput["result"] != "success" {
		t.Errorf("result = %v", decodedOutput["result"])
	}
}

func TestPayloadWithNullValues(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-nullval", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	input := map[string]interface{}{
		"key": nil,
		"arr": []interface{}{1, nil, 3},
	}

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, input, nil)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if steps[0].InputJSON == nil {
		t.Error("expected non-nil input_json with null values")
	}
}

func TestPayloadWithLargeStrings(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-bigstr", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	bigString := make([]byte, 100*1024)
	for i := range bigString {
		bigString[i] = byte('a' + (i % 26))
	}
	payload := map[string]string{"data": string(bigString)}

	err := e.CompleteStep(wf.WorkflowID, "w1", 1, payload, payload)
	if err != nil {
		t.Fatal(err)
	}

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 {
		t.Fatal("expected 1 step")
	}
	if steps[0].InputJSON == nil || len(*steps[0].InputJSON) < 100*1024 {
		t.Error("input_json too short for large payload")
	}
}

func TestSignalWithNilPayload(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-sig-nil", "t")

	sig, err := e.SendSignal("wf-sig-nil", "empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sig.PayloadJSON != nil {
		t.Errorf("expected nil payload, got %v", *sig.PayloadJSON)
	}

	consumed, _ := e.ConsumeSignal("wf-sig-nil")
	if consumed == nil {
		t.Fatal("expected signal")
	}
	if consumed.PayloadJSON != nil {
		t.Errorf("expected nil payload on consume, got %v", *consumed.PayloadJSON)
	}
}

func TestSignalWithComplexPayload(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-sig-complex", "t")

	payload := map[string]interface{}{
		"event":    "order.confirmed",
		"order_id": "ord-12345",
		"amount":   99.99,
		"items":    []string{"item-a", "item-b"},
		"nested": map[string]bool{
			"gift_wrap": true,
			"express":   false,
		},
	}

	_, err := e.SendSignal("wf-sig-complex", "order_event", payload)
	if err != nil {
		t.Fatal(err)
	}

	consumed, _ := e.ConsumeSignal("wf-sig-complex")
	if consumed == nil {
		t.Fatal("expected signal")
	}

	var decoded map[string]interface{}
	json.Unmarshal([]byte(*consumed.PayloadJSON), &decoded)
	if decoded["order_id"] != "ord-12345" {
		t.Errorf("order_id = %v", decoded["order_id"])
	}
}

func TestAvgStepDurationNoData(t *testing.T) {
	e := newTestEngine(t)

	avg, err := e.AvgStepDuration()
	if err != nil {
		t.Fatal(err)
	}
	if len(avg) != 0 {
		t.Errorf("expected empty map, got %v", avg)
	}
}

func TestAvgStepDurationWithData(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-avg", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	time.Sleep(50 * time.Millisecond)

	avg, err := e.AvgStepDuration()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := avg[1]; !ok {
		t.Errorf("expected avg for step 1, got %v", avg)
	}
}

func TestAvgStepDurationMultipleSteps(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-avg2", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
	e.CompleteStep(wf.WorkflowID, "w1", 2, nil, nil)

	avg, err := e.AvgStepDuration()
	if err != nil {
		t.Fatal(err)
	}
	if len(avg) < 1 {
		t.Errorf("expected at least 1 step avg, got %v", avg)
	}
}

func TestQueryEmptyWorkflows(t *testing.T) {
	e := newTestEngine(t)

	queued, _ := e.QueryQueued()
	running, _ := e.QueryRunning()
	waiting, _ := e.QueryWaiting()
	failed, _ := e.QueryFailed()

	if len(queued) != 0 {
		t.Error("expected 0 queued")
	}
	if len(running) != 0 {
		t.Error("expected 0 running")
	}
	if len(waiting) != 0 {
		t.Error("expected 0 waiting")
	}
	if len(failed) != 0 {
		t.Error("expected 0 failed")
	}
}

func TestTimerWithZeroTime(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-tmr-zero", "t")

	timer, err := e.CreateTimer("wf-tmr-zero", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if timer.WakeAt != 0 {
		t.Errorf("expected wake_at=0, got %d", timer.WakeAt)
	}
}

func TestTimerWithNegativeTime(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-tmr-neg", "t")

	timer, err := e.CreateTimer("wf-tmr-neg", time.Unix(-1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if timer.WakeAt != -1 {
		t.Errorf("expected wake_at=-1, got %d", timer.WakeAt)
	}
}

func TestTimerWithFutureTime(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-tmr-future", "t")

	future := time.Now().Add(24 * 365 * time.Hour)
	timer, err := e.CreateTimer("wf-tmr-future", future)
	if err != nil {
		t.Fatal(err)
	}
	if timer.Fired {
		t.Error("future timer should not be fired")
	}

	pending, _ := e.GetPendingTimers("wf-tmr-future")
	if len(pending) != 1 {
		t.Errorf("expected 1 pending timer, got %d", len(pending))
	}
}

func TestSendSignalEmptyType(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-empty-sig", "t")

	sig, err := e.SendSignal("wf-empty-sig", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sig.SignalType != "" {
		t.Errorf("expected empty signal type, got %q", sig.SignalType)
	}
}

func TestConsumeSignalFromEmptyQueue(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-empty-queue", "t")

	sig, err := e.ConsumeSignal("wf-empty-queue")
	if err != nil {
		t.Fatal(err)
	}
	if sig != nil {
		t.Errorf("expected nil signal from empty queue, got %+v", sig)
	}
}

func TestMultipleSignalsOrderPreservation(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-order", "t")

	types := []string{"first", "second", "third", "fourth", "fifth"}
	for _, typ := range types {
		e.SendSignal("wf-order", typ, nil)
	}

	for i, expected := range types {
		sig, err := e.ConsumeSignal("wf-order")
		if err != nil {
			t.Fatal(err)
		}
		if sig == nil {
			t.Fatalf("expected signal %d (%q), got nil", i, expected)
		}
		if sig.SignalType != expected {
			t.Errorf("signal %d: expected %q, got %q", i, expected, sig.SignalType)
		}
	}
}

func TestNewSchedulerDefaults(t *testing.T) {
	e := newTestEngine(t)
	s := NewScheduler(e, 0)
	if s.interval != DefaultPollInterval {
		t.Errorf("expected default interval %v, got %v", DefaultPollInterval, s.interval)
	}

	s = NewScheduler(e, -5*time.Second)
	if s.interval != DefaultPollInterval {
		t.Errorf("expected default interval for negative input, got %v", s.interval)
	}
}

func TestNewSchedulerCustomInterval(t *testing.T) {
	e := newTestEngine(t)
	s := NewScheduler(e, 500*time.Millisecond)
	if s.interval != 500*time.Millisecond {
		t.Errorf("expected 500ms, got %v", s.interval)
	}
}
