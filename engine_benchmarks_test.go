package durable

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func BenchmarkCreateWorkflow(b *testing.B) {
	e := newTestEngineB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("b-create-%d", i)
		_, err := e.CreateWorkflow(id, "bench")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetWorkflow(b *testing.B) {
	e := newTestEngineB(b)
	e.CreateWorkflow("b-get", "bench")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.GetWorkflow("b-get")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAcquireLease(b *testing.B) {
	e := newTestEngineB(b)
	for i := 0; i < b.N; i++ {
		e.CreateWorkflow(fmt.Sprintf("b-acq-%d", i), "bench")
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wf, err := e.AcquireLease(fmt.Sprintf("w-%d", i%100), 10*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if wf != nil {
			e.CompleteWorkflow(wf.WorkflowID, fmt.Sprintf("w-%d", i%100))
		}
	}
}

func BenchmarkCompleteStep(b *testing.B) {
	e := newTestEngineB(b)
	wf, _ := e.CreateWorkflow("b-cstep", "bench")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := e.CompleteStep(wf.WorkflowID, "w1", i+1, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStartAndCompleteStep(b *testing.B) {
	e := newTestEngineB(b)
	wf, _ := e.CreateWorkflow("b-scstep", "bench")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.StartStep(wf.WorkflowID, "w1", i+1, nil)
		err := e.CompleteStep(wf.WorkflowID, "w1", i+1, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSendSignal(b *testing.B) {
	e := newTestEngineB(b)
	e.CreateWorkflow("b-sig", "bench")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.SendSignal("b-sig", "ping", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeSignal(b *testing.B) {
	e := newTestEngineB(b)
	e.CreateWorkflow("b-consig", "bench")

	for i := 0; i < b.N; i++ {
		e.SendSignal("b-consig", "ping", nil)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.ConsumeSignal("b-consig")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreateTimer(b *testing.B) {
	e := newTestEngineB(b)
	e.CreateWorkflow("b-tmr", "bench")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.CreateTimer("b-tmr", time.Now().Add(1*time.Hour))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFailStep(b *testing.B) {
	e := newTestEngineB(b)
	wf, _ := e.CreateWorkflow("b-fstep", "bench")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := e.FailStep(wf.WorkflowID, "w1", i+1, nil, fmt.Errorf("bench error"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompleteWorkflow(b *testing.B) {
	e := newTestEngineB(b)
	for i := 0; i < b.N; i++ {
		e.CreateWorkflow(fmt.Sprintf("b-comp-%d", i), "bench")
		e.AcquireLease("w1", 10*time.Second)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := e.CompleteWorkflow(fmt.Sprintf("b-comp-%d", i), "w1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFailWorkflow(b *testing.B) {
	e := newTestEngineB(b)
	for i := 0; i < b.N; i++ {
		e.CreateWorkflow(fmt.Sprintf("b-fail-%d", i), "bench")
		e.AcquireLease("w1", 10*time.Second)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := e.FailWorkflow(fmt.Sprintf("b-fail-%d", i), "w1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetEvents(b *testing.B) {
	e := newTestEngineB(b)
	wf, _ := e.CreateWorkflow("b-events", "bench")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	for i := 0; i < 100; i++ {
		e.CompleteStep(wf.WorkflowID, "w1", i+1, nil, nil)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.GetEvents("b-events")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSteps(b *testing.B) {
	e := newTestEngineB(b)
	wf, _ := e.CreateWorkflow("b-steps", "bench")
	wf, _ = e.AcquireLease("w1", 10*time.Second)
	for i := 0; i < 100; i++ {
		e.CompleteStep(wf.WorkflowID, "w1", i+1, nil, nil)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.GetSteps("b-steps")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFullWorkflowLifecycle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e := newTestEngineB(b)
		b.StartTimer()

		wf, _ := e.CreateWorkflow(fmt.Sprintf("b-lifecycle-%d", i), "bench")
		wf, _ = e.AcquireLease("w1", 10*time.Second)

		for s := 1; s <= 5; s++ {
			e.StartStep(wf.WorkflowID, "w1", s, nil)
			e.CompleteStep(wf.WorkflowID, "w1", s, nil, nil)
		}
		e.CompleteWorkflow(wf.WorkflowID, "w1")
	}
}

func BenchmarkFullWorkflowWithRetry(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e := newTestEngineB(b)
		b.StartTimer()

		wf, _ := e.CreateWorkflow(fmt.Sprintf("b-retry-%d", i), "bench")
		wf, _ = e.AcquireLease("w1", 10*time.Second)

		e.CompleteStep(wf.WorkflowID, "w1", 1, nil, nil)
		e.FailStep(wf.WorkflowID, "w1", 2, nil, errors.New("bench error"))

		cfg := RetryConfig{MaxRetries: 3, Policy: FixedBackoff{1}}
		e.RetryStep(wf.WorkflowID, "w1", 2, 1, cfg)

		e.CompleteWorkflow(wf.WorkflowID, "w1")
	}
}

func BenchmarkConcurrentCreateWorkflow(b *testing.B) {
	e := newTestEngineB(b)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.CreateWorkflow(fmt.Sprintf("b-ccw-%d-%d", i, time.Now().UnixNano()), "bench")
			i++
		}
	})
}

func BenchmarkConcurrentAcquireLease(b *testing.B) {
	e := newTestEngineB(b)
	for i := 0; i < b.N; i++ {
		e.CreateWorkflow(fmt.Sprintf("b-cal-%d", i), "bench")
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		id := 0
		for pb.Next() {
			wf, _ := e.AcquireLease(fmt.Sprintf("w-%d", id), 10*time.Second)
			if wf != nil {
				e.CompleteWorkflow(wf.WorkflowID, fmt.Sprintf("w-%d", id))
			}
			id++
		}
	})
}

func BenchmarkConcurrentSendSignal(b *testing.B) {
	e := newTestEngineB(b)
	e.CreateWorkflow("b-csig", "bench")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.SendSignal("b-csig", "ping", nil)
		}
	})
}

func BenchmarkSchedulerTickEmpty(b *testing.B) {
	e := newTestEngineB(b)
	s := NewScheduler(e, 100*time.Millisecond)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Tick()
	}
}

func BenchmarkAcquireLeaseNoWorkAvailable(b *testing.B) {
	e := newTestEngineB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.AcquireLease("w1", 10*time.Second)
	}
}

func newTestEngineB(b *testing.B) *Engine {
	b.Helper()
	path := b.TempDir() + "/test.db"
	engine, err := New(path)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return engine
}
