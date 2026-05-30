package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	durable "github.com/nnn/sqlite-durable-workflow"
)

var (
	dbPath = flag.String("db", "workflows.db", "path to SQLite database")
	addr   = flag.String("addr", ":8080", "HTTP listen address")
	tick   = flag.Duration("tick", 1*time.Second, "scheduler poll interval")
	lease  = flag.Duration("lease", 30*time.Second, "default lease duration")
	apiKey = flag.String("api-key", "", "require Bearer token matching this value (empty = no auth)")
)

func main() {
	flag.Parse()

	engine, err := durable.New(*dbPath)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	defer engine.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go durable.NewScheduler(engine, *tick).Run(ctx)

	srv := &server{engine: engine, leaseDuration: *lease}

	mux := http.NewServeMux()

	// Workflows.
	mux.HandleFunc("POST /workflows", srv.createWorkflow)
	mux.HandleFunc("GET /workflows/{id}", srv.getWorkflow)
	mux.HandleFunc("POST /workflows/{id}/complete", srv.completeWorkflow)
	mux.HandleFunc("POST /workflows/{id}/fail", srv.failWorkflow)
	mux.HandleFunc("POST /workflows/{id}/waiting", srv.markWaiting)

	// Leases.
	mux.HandleFunc("POST /leases/acquire", srv.acquireLease)
	mux.HandleFunc("POST /leases/{id}/renew", srv.renewLease)
	mux.HandleFunc("POST /leases/{id}/release", srv.releaseLease)

	// Steps.
	mux.HandleFunc("POST /steps/start", srv.startStep)
	mux.HandleFunc("POST /steps/complete", srv.completeStep)
	mux.HandleFunc("POST /steps/fail", srv.failStep)
	mux.HandleFunc("GET /steps/{id}", srv.getSteps)

	// Signals.
	mux.HandleFunc("POST /signals", srv.sendSignal)
	mux.HandleFunc("POST /signals/{id}/consume", srv.consumeSignal)
	mux.HandleFunc("GET /signals/{id}", srv.getSignals)

	// Timers.
	mux.HandleFunc("POST /timers", srv.createTimer)

	// Retry.
	mux.HandleFunc("POST /retry", srv.retryStep)

	// Events.
	mux.HandleFunc("GET /events/{id}", srv.getEvents)

	// Observability.
	mux.HandleFunc("GET /observability/failed", srv.queryFailed)
	mux.HandleFunc("GET /observability/running", srv.queryRunning)
	mux.HandleFunc("GET /observability/queued", srv.queryQueued)
	mux.HandleFunc("GET /observability/waiting", srv.queryWaiting)
	mux.HandleFunc("GET /observability/avg-step-duration", srv.avgStepDuration)

	handler := http.Handler(mux)
	if *apiKey != "" {
		handler = requireBearer(mux, *apiKey)
	}

	hs := &http.Server{Addr: *addr, Handler: handler}

	go func() {
		log.Printf("listening on %s (db=%s)", *addr, *dbPath)
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, sdCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sdCancel()
	hs.Shutdown(shutdownCtx)
}

type server struct {
	engine        *durable.Engine
	leaseDuration time.Duration
}

func requireBearer(next http.Handler, key string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+key {
			writeError(w, 401, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── helpers ──────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func decode(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// ── workflows ────────────────────────────────────────────────────

type createWorkflowReq struct {
	WorkflowID   string `json:"workflow_id"`
	WorkflowType string `json:"workflow_type"`
}

func (s *server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var req createWorkflowReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	wf, err := s.engine.CreateWorkflow(req.WorkflowID, req.WorkflowType)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, wf)
}

func (s *server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wf, err := s.engine.GetWorkflow(id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, wf)
}

type workerReq struct {
	WorkerID string `json:"worker_id"`
}

func (s *server) completeWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req workerReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.CompleteWorkflow(id, req.WorkerID); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) failWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req workerReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.FailWorkflow(id, req.WorkerID); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) markWaiting(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req workerReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.MarkWaiting(id, req.WorkerID); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// ── leases ───────────────────────────────────────────────────────

type acquireReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration"` // seconds, optional
}

func (s *server) acquireLease(w http.ResponseWriter, r *http.Request) {
	var req acquireReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	dur := s.leaseDuration
	if req.LeaseDuration > 0 {
		dur = time.Duration(req.LeaseDuration) * time.Second
	}
	wf, err := s.engine.AcquireLease(req.WorkerID, dur)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if wf == nil {
		writeJSON(w, map[string]string{"status": "no_work"})
		return
	}
	writeJSON(w, wf)
}

type renewReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration"` // seconds, optional
}

func (s *server) renewLease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req renewReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	dur := s.leaseDuration
	if req.LeaseDuration > 0 {
		dur = time.Duration(req.LeaseDuration) * time.Second
	}
	if err := s.engine.RenewLease(id, req.WorkerID, dur); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) releaseLease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req workerReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.ReleaseLease(id, req.WorkerID); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// ── steps ────────────────────────────────────────────────────────

type stepReq struct {
	WorkflowID string      `json:"workflow_id"`
	WorkerID   string      `json:"worker_id"`
	StepNumber int         `json:"step_number"`
	Input      interface{} `json:"input"`
}

func (s *server) startStep(w http.ResponseWriter, r *http.Request) {
	var req stepReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.StartStep(req.WorkflowID, req.WorkerID, req.StepNumber, req.Input); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

type completeStepReq struct {
	WorkflowID string      `json:"workflow_id"`
	WorkerID   string      `json:"worker_id"`
	StepNumber int         `json:"step_number"`
	Input      interface{} `json:"input"`
	Output     interface{} `json:"output"`
}

func (s *server) completeStep(w http.ResponseWriter, r *http.Request) {
	var req completeStepReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.CompleteStep(req.WorkflowID, req.WorkerID, req.StepNumber, req.Input, req.Output); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

type failStepReq struct {
	WorkflowID string      `json:"workflow_id"`
	WorkerID   string      `json:"worker_id"`
	StepNumber int         `json:"step_number"`
	Input      interface{} `json:"input"`
	Error      string      `json:"error"`
}

type strError struct{ msg string }

func (e strError) Error() string { return e.msg }

func (s *server) failStep(w http.ResponseWriter, r *http.Request) {
	var req failStepReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.engine.FailStep(req.WorkflowID, req.WorkerID, req.StepNumber, req.Input, strError{req.Error}); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) getSteps(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	steps, err := s.engine.GetSteps(id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, steps)
}

// ── signals ──────────────────────────────────────────────────────

type signalReq struct {
	WorkflowID string      `json:"workflow_id"`
	SignalType string      `json:"signal_type"`
	Payload    interface{} `json:"payload"`
}

func (s *server) sendSignal(w http.ResponseWriter, r *http.Request) {
	var req signalReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	sig, err := s.engine.SendSignal(req.WorkflowID, req.SignalType, req.Payload)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, sig)
}

func (s *server) consumeSignal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sig, err := s.engine.ConsumeSignal(id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if sig == nil {
		writeJSON(w, map[string]string{"status": "no_signal"})
		return
	}
	writeJSON(w, sig)
}

func (s *server) getSignals(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unconsumedOnly := r.URL.Query().Get("unconsumed") == "true"
	signals, err := s.engine.GetSignals(id, unconsumedOnly)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, signals)
}

// ── timers ───────────────────────────────────────────────────────

type timerReq struct {
	WorkflowID  string `json:"workflow_id"`
	WakeAtEpoch int64  `json:"wake_at"` // unix seconds
}

func (s *server) createTimer(w http.ResponseWriter, r *http.Request) {
	var req timerReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	timer, err := s.engine.CreateTimer(req.WorkflowID, time.Unix(req.WakeAtEpoch, 0))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, timer)
}

// ── retry ────────────────────────────────────────────────────────

type retryReq struct {
	WorkflowID string `json:"workflow_id"`
	WorkerID   string `json:"worker_id"`
	StepNumber int    `json:"step_number"`
	RetryCount int    `json:"retry_count"`
	MaxRetries int    `json:"max_retries"`
	Policy     string `json:"policy"`       // fixed, linear, exponential
	BaseSecs   int64  `json:"base_seconds"` // base delay in seconds
	MaxSecs    int64  `json:"max_seconds"`  // cap for exponential
}

func (s *server) retryStep(w http.ResponseWriter, r *http.Request) {
	var req retryReq
	if err := decode(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}

	var policy durable.RetryPolicy
	switch req.Policy {
	case "linear":
		policy = durable.LinearBackoff{BaseSeconds: req.BaseSecs}
	case "exponential":
		policy = durable.ExponentialBackoff{BaseSeconds: req.BaseSecs, MaxSeconds: req.MaxSecs}
	default:
		policy = durable.FixedBackoff{DelaySeconds: req.BaseSecs}
	}

	cfg := durable.RetryConfig{MaxRetries: req.MaxRetries, Policy: policy}
	ok, err := s.engine.RetryStep(req.WorkflowID, req.WorkerID, req.StepNumber, req.RetryCount, cfg)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, map[string]interface{}{"retry_scheduled": ok})
}

// ── events ───────────────────────────────────────────────────────

func (s *server) getEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	events, err := s.engine.GetEvents(id)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, events)
}

// ── observability ────────────────────────────────────────────────

func (s *server) queryFailed(w http.ResponseWriter, r *http.Request) {
	list, _ := s.engine.QueryFailed()
	writeJSON(w, withCount(list))
}

func (s *server) queryRunning(w http.ResponseWriter, r *http.Request) {
	list, _ := s.engine.QueryRunning()
	writeJSON(w, withCount(list))
}

func (s *server) queryQueued(w http.ResponseWriter, r *http.Request) {
	list, _ := s.engine.QueryQueued()
	writeJSON(w, withCount(list))
}

func (s *server) queryWaiting(w http.ResponseWriter, r *http.Request) {
	list, _ := s.engine.QueryWaiting()
	writeJSON(w, withCount(list))
}

func (s *server) avgStepDuration(w http.ResponseWriter, r *http.Request) {
	avg, _ := s.engine.AvgStepDuration()
	writeJSON(w, avg)
}

func withCount(v interface{}) map[string]interface{} {
	if v == nil {
		return map[string]interface{}{"items": []interface{}{}, "count": 0}
	}
	j, _ := json.Marshal(v)
	var arr []interface{}
	json.Unmarshal(j, &arr)
	if arr == nil {
		arr = []interface{}{}
	}
	return map[string]interface{}{"items": arr, "count": len(arr)}
}
