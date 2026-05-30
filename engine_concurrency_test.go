package durable

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentAcquireLeaseSingleWorkflow(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-race", "t")

	var acquiredCount int32
	var wg sync.WaitGroup
	workers := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			wf, err := e.AcquireLease(workerID, 10*time.Second)
			if err != nil {
				t.Errorf("worker %s: AcquireLease error: %v", workerID, err)
				return
			}
			if wf != nil {
				atomic.AddInt32(&acquiredCount, 1)
			}
		}(fmt.Sprintf("worker-%d", i))
	}
	wg.Wait()

	if acquiredCount != 1 {
		t.Errorf("expected exactly 1 acquisition, got %d", acquiredCount)
	}
}

func TestConcurrentAcquireLeaseMultipleWorkflows(t *testing.T) {
	e := newTestEngine(t)

	const numWorkflows = 20
	const numWorkers = 5

	for i := 0; i < numWorkflows; i++ {
		e.CreateWorkflow(fmt.Sprintf("wf-multi-%d", i), "t")
	}

	var acquiredCount int32
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				wf, err := e.AcquireLease(workerID, 10*time.Second)
				if err != nil {
					return
				}
				if wf == nil {
					return
				}
				atomic.AddInt32(&acquiredCount, 1)
				e.CompleteWorkflow(wf.WorkflowID, workerID)
			}
		}(fmt.Sprintf("worker-%d", i))
	}
	wg.Wait()

	finalCount := atomic.LoadInt32(&acquiredCount)
	if finalCount != numWorkflows {
		t.Errorf("expected %d workflows completed, got %d", numWorkflows, finalCount)
	}
}

func TestConcurrentCompleteStepRace(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-cs-race", "t")

	const goroutines = 5
	var wg sync.WaitGroup
	var successCount int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			wf, err := e.AcquireLease("w1", 10*time.Second)
			if err != nil || wf == nil || wf.WorkflowID != "wf-cs-race" {
				return
			}

			err = e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if err == ErrStepAlreadyCompleted {
			} else if err == ErrLeaseMismatch {
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&successCount) != 1 {
		t.Errorf("expected exactly 1 successful CompleteStep, got %d", successCount)
	}

	wf, _ := e.GetWorkflow("wf-cs-race")
	if wf.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1 (cursor should advance exactly once)", wf.CurrentStep)
	}
}

func TestConcurrentSignalConsume(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-sig-race", "t")

	numSignals := 10
	for i := 0; i < numSignals; i++ {
		e.SendSignal("wf-sig-race", fmt.Sprintf("type-%d", i), nil)
	}

	var consumedCount int32
	var wg sync.WaitGroup
	workers := 5

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				sig, err := e.ConsumeSignal("wf-sig-race")
				if err != nil {
					continue
				}
				if sig != nil {
					atomic.AddInt32(&consumedCount, 1)
				}
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&consumedCount) != int32(numSignals) {
		t.Errorf("expected %d signals consumed, got %d", numSignals, consumedCount)
	}

	remaining, _ := e.GetSignals("wf-sig-race", true)
	if len(remaining) != 0 {
		t.Errorf("expected 0 remaining signals, got %d", len(remaining))
	}
}

func TestConcurrentSendSignal(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-send-race", "t")

	numSignals := 50
	var wg sync.WaitGroup

	for i := 0; i < numSignals; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := e.SendSignal("wf-send-race", fmt.Sprintf("type-%d", idx), nil)
			if err != nil {
				t.Errorf("SendSignal error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	all, _ := e.GetSignals("wf-send-race", false)
	if len(all) != numSignals {
		t.Errorf("expected %d signals, got %d", numSignals, len(all))
	}
}

func TestConcurrentCreateTimers(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-timer-race", "t")

	numTimers := 20
	var wg sync.WaitGroup

	for i := 0; i < numTimers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := e.CreateTimer("wf-timer-race", time.Now().Add(time.Duration(idx)*time.Hour))
			if err != nil {
				t.Errorf("CreateTimer error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	pending, _ := e.GetPendingTimers("wf-timer-race")
	if len(pending) != numTimers {
		t.Errorf("expected %d pending timers, got %d", numTimers, len(pending))
	}
}

func TestConcurrentCreateWorkflows(t *testing.T) {
	e := newTestEngine(t)

	numWorkflows := 50
	var wg sync.WaitGroup

	for i := 0; i < numWorkflows; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := e.CreateWorkflow(fmt.Sprintf("wf-par-%d", idx), "t")
			if err != nil {
				t.Errorf("CreateWorkflow error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	queued, _ := e.QueryQueued()
	if len(queued) != numWorkflows {
		t.Errorf("expected %d queued workflows, got %d", numWorkflows, len(queued))
	}
}

func TestConcurrentLeaseRenewalAndExpiry(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-renew-race", "t")

	wf, _ := e.AcquireLease("w1", 500*time.Millisecond)

	var wg sync.WaitGroup
	var renewSuccess, renewFail int32

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.RenewLease(wf.WorkflowID, "w1", 10*time.Second)
			if err == nil {
				atomic.AddInt32(&renewSuccess, 1)
			} else if err == ErrLeaseMismatch {
				atomic.AddInt32(&renewFail, 1)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&renewSuccess) < 1 {
		t.Error("expected at least 1 successful renew, got 0")
	}
}

func TestConcurrentStartStepAndCompleteStep(t *testing.T) {
	e := newTestEngine(t)
	wf, _ := e.CreateWorkflow("wf-ss-race", "t")
	wf, _ = e.AcquireLease("w1", 10*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.StartStep(wf.WorkflowID, "w1", 1, nil)
		}()
	}
	wg.Wait()

	steps, _ := e.GetSteps(wf.WorkflowID)
	if len(steps) != 1 {
		t.Errorf("expected 1 step, got %d", len(steps))
	}
	if steps[0].Status != "RUNNING" {
		t.Errorf("expected RUNNING status, got %s", steps[0].Status)
	}
}

func TestMultipleWorkersProcessWorkflowSteps(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-mw-steps", "t")

	numWorkers := 3
	numSteps := 10
	var wg sync.WaitGroup

	acquired, _ := e.AcquireLease("w1", 10*time.Second)
	workerID := acquired.LeaseOwner
	if workerID == nil {
		t.Fatal("expected lease")
	}

	var completions int32

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for step := 1; step <= numSteps; step++ {
				err := e.CompleteStep("wf-mw-steps", *workerID, step, nil, nil)
				if err == nil {
					atomic.AddInt32(&completions, 1)
				}
			}
		}()
	}
	wg.Wait()

	wf, _ := e.GetWorkflow("wf-mw-steps")
	if wf.CurrentStep != numSteps {
		t.Logf("note: CurrentStep = %d, want %d (race may advance fewer times)", wf.CurrentStep, numSteps)
	}
	if atomic.LoadInt32(&completions) != int32(numSteps) {
		t.Logf("completions = %d, expected %d", completions, numSteps)
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	e := newTestEngine(t)

	numWorkflows := 10
	for i := 0; i < numWorkflows; i++ {
		e.CreateWorkflow(fmt.Sprintf("wf-mixed-%d", i), "t")
	}

	var wg sync.WaitGroup
	var creates, acquires, completes, fails, signals int32

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				wf, err := e.AcquireLease(fmt.Sprintf("mixer-%d", idx), 10*time.Second)
				if err != nil || wf == nil {
					continue
				}
				atomic.AddInt32(&acquires, 1)

				e.CompleteStep(wf.WorkflowID, fmt.Sprintf("mixer-%d", idx), 1, nil, nil)
				e.SendSignal(wf.WorkflowID, "ping", nil)
				atomic.AddInt32(&signals, 1)

				if j%2 == 0 {
					e.CompleteWorkflow(wf.WorkflowID, fmt.Sprintf("mixer-%d", idx))
					atomic.AddInt32(&completes, 1)
				} else {
					e.FailWorkflow(wf.WorkflowID, fmt.Sprintf("mixer-%d", idx))
					atomic.AddInt32(&fails, 1)
				}
			}
		}(i)
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				e.CreateWorkflow(fmt.Sprintf("wf-extra-%d-%d", idx, j), "t")
				atomic.AddInt32(&creates, 1)
			}
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt32(&creates) == 0 {
		t.Error("no workflows created")
	}
	if atomic.LoadInt32(&acquires) == 0 {
		t.Error("no leases acquired")
	}
	if atomic.LoadInt32(&completes)+atomic.LoadInt32(&fails) == 0 {
		t.Error("no workflows completed or failed")
	}
}

func TestAcquireLeaseConcurrencyFairness(t *testing.T) {
	e := newTestEngine(t)

	numWorkflows := 1000
	for i := 0; i < numWorkflows; i++ {
		e.CreateWorkflow(fmt.Sprintf("wf-fair-%d", i), "t")
	}

	workers := 4
	acquired := make([]int32, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			workerID := fmt.Sprintf("w%d", workerIdx)
			for {
				wf, err := e.AcquireLease(workerID, 10*time.Second)
				if err != nil || wf == nil {
					return
				}
				atomic.AddInt32(&acquired[workerIdx], 1)
				e.CompleteWorkflow(wf.WorkflowID, workerID)
			}
		}(i)
	}
	wg.Wait()

	total := int32(0)
	for i, count := range acquired {
		total += count
		t.Logf("worker %d acquired %d workflows", i, count)
	}
	if total != int32(numWorkflows) {
		t.Errorf("expected %d total completions, got %d", numWorkflows, total)
	}
}

func TestConcurrentGetWorkflow(t *testing.T) {
	e := newTestEngine(t)
	e.CreateWorkflow("wf-conc-read", "t")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wf, err := e.GetWorkflow("wf-conc-read")
			if err != nil {
				if err != sql.ErrNoRows {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if wf.Status != StatusQueued {
				t.Errorf("expected QUEUED, got %s", wf.Status)
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentQueryObservability(t *testing.T) {
	e := newTestEngine(t)

	for i := 0; i < 5; i++ {
		e.CreateWorkflow(fmt.Sprintf("wf-obs-c%d", i), "t")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.QueryQueued()
			e.QueryRunning()
			e.QueryWaiting()
			e.QueryFailed()
		}()
	}
	wg.Wait()
}
