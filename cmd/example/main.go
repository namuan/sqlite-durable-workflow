package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	durable "github.com/nnn/sqlite-durable-workflow"
)

func main() {
	dbPath := "example.db"

	// Clean up from any previous run.
	os.Remove(dbPath)
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	engine, err := durable.New(dbPath)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}

	// ── Scheduler ──────────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)

	sched := durable.NewScheduler(engine, 500*time.Millisecond)
	go sched.Run(ctx)

	// Ensure cleanup runs in correct order: scheduler → engine → db file.
	defer func() {
		cancel()
		engine.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")
	}()

	// ── Create workflows ───────────────────────────────────────
	wf1, err := engine.CreateWorkflow("order-001", "order_processing")
	if err != nil {
		log.Fatalf("create wf1: %v", err)
	}
	wf2, err := engine.CreateWorkflow("order-002", "order_processing")
	if err != nil {
		log.Fatalf("create wf2: %v", err)
	}
	fmt.Printf("[create] %s → %s\n", wf1.WorkflowID, wf1.Status)
	fmt.Printf("[create] %s → %s\n", wf2.WorkflowID, wf2.Status)

	// ── Worker A acquires first workflow ───────────────────────
	wf, err := engine.AcquireLease("worker-a", 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[lease]  %s claimed by worker-a → %s\n", wf.WorkflowID, wf.Status)

	// ── Execute steps with retry on failure ────────────────────
	retryCfg := durable.RetryConfig{
		MaxRetries: 2,
		Policy:     durable.ExponentialBackoff{BaseSeconds: 1, MaxSeconds: 10},
	}

	for step := 1; step <= 3; step++ {
		input := map[string]interface{}{"step": step}
		engine.StartStep(wf.WorkflowID, "worker-a", step, input)

		if step == 2 {
			fmt.Printf("[step]   step %d failed — scheduling retry\n", step)
			ok, err := engine.RetryStep(wf.WorkflowID, "worker-a", step, 1, retryCfg)
			if err != nil {
				log.Fatalf("retry: %v", err)
			}
			if !ok {
				engine.FailWorkflow(wf.WorkflowID, "worker-a")
				return
			}
			break
		}

		output := map[string]string{"result": fmt.Sprintf("ok-%d", step)}
		if err := engine.CompleteStep(wf.WorkflowID, "worker-a", step, input, output); err != nil {
			log.Fatalf("complete step %d: %v", step, err)
		}
		fmt.Printf("[step]   step %d completed\n", step)
	}

	// ── Send a signal to the second workflow ───────────────────
	sig, err := engine.SendSignal("order-002", "approval_requested",
		map[string]string{"approved_by": "manager"})
	if err != nil {
		log.Fatalf("signal: %v", err)
	}
	fmt.Printf("[signal] sent signal %d to order-002\n", sig.SignalID)

	pending, _ := engine.GetSignals("order-002", true)
	fmt.Printf("[signal] pending signals for order-002: %d\n", len(pending))

	// ── Create a delayed timer ──────────────────────────────────
	timer, err := engine.CreateTimer("order-002", time.Now().Add(2*time.Second))
	if err != nil {
		log.Fatalf("timer: %v", err)
	}
	fmt.Printf("[timer]  created timer %d wakes at %s\n",
		timer.TimerID, time.Unix(timer.WakeAt, 0).Format("15:04:05"))

	// ── Wait for scheduler to process things ───────────────────
	time.Sleep(3 * time.Second)

	// ── Re-acquire the retried workflow ─────────────────────────
	wf, err = engine.AcquireLease("worker-a", 10*time.Second)
	if err != nil {
		log.Fatalf("acquire: %v", err)
	}
	fmt.Printf("[retry]  re-acquired %s (step %d)\n", wf.WorkflowID, wf.CurrentStep)

	// Finish remaining steps.
	for step := wf.CurrentStep + 1; step <= 3; step++ {
		input := map[string]interface{}{"step": step}
		output := map[string]string{"result": fmt.Sprintf("ok-%d", step)}
		if err := engine.CompleteStep(wf.WorkflowID, "worker-a", step, input, output); err != nil {
			log.Fatalf("complete step %d: %v", step, err)
		}
		fmt.Printf("[step]   step %d completed\n", step)
	}
	if err := engine.CompleteWorkflow(wf.WorkflowID, "worker-a"); err != nil {
		log.Fatalf("complete: %v", err)
	}
	fmt.Printf("[done]   %s → COMPLETED\n", wf.WorkflowID)

	// ── Observability ──────────────────────────────────────────
	fmt.Println("\n── Observability ──")

	failed, _ := engine.QueryFailed()
	fmt.Printf("  failed:   %d\n", len(failed))

	running, _ := engine.QueryRunning()
	fmt.Printf("  running:  %d\n", len(running))

	queued, _ := engine.QueryQueued()
	fmt.Printf("  queued:   %d\n", len(queued))

	waiting, _ := engine.QueryWaiting()
	fmt.Printf("  waiting:  %d\n", len(waiting))

	avg, _ := engine.AvgStepDuration()
	fmt.Printf("  avg step: %s\n", formatAvg(avg))

	// ── Show event history ─────────────────────────────────────
	events, err := engine.GetEvents("order-001")
	if err != nil {
		log.Fatalf("events: %v", err)
	}
	fmt.Printf("\n  events for order-001:\n")
	for _, ev := range events {
		payload := "<nil>"
		if ev.PayloadJSON != nil {
			payload = *ev.PayloadJSON
		}
		fmt.Printf("    [%d] %s %s\n", ev.SequenceNumber, ev.EventType, payload)
	}
}

func formatAvg(m map[int]float64) string {
	b, _ := json.Marshal(m)
	return string(b)
}
