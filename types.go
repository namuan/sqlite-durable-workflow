package durable

import "encoding/json"

// Workflow status constants.
const (
	StatusQueued    = "QUEUED"
	StatusRunning   = "RUNNING"
	StatusWaiting   = "WAITING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Event type constants.
const (
	EventWorkflowCreated   = "WORKFLOW_CREATED"
	EventWorkflowStarted   = "WORKFLOW_STARTED"
	EventStepStarted       = "STEP_STARTED"
	EventStepCompleted     = "STEP_COMPLETED"
	EventStepFailed        = "STEP_FAILED"
	EventSignalReceived    = "SIGNAL_RECEIVED"
	EventTimerFired        = "TIMER_FIRED"
	EventWorkflowCompleted = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    = "WORKFLOW_FAILED"
)

// Workflow represents a row in the workflows table.
type Workflow struct {
	WorkflowID     string  `json:"workflow_id"`
	WorkflowType   string  `json:"workflow_type"`
	Status         string  `json:"status"`
	CurrentStep    int     `json:"current_step"`
	LeaseOwner     *string `json:"lease_owner"`
	LeaseExpiresAt *int64  `json:"lease_expires_at"`
	CreatedAt      int64   `json:"created_at"`
	UpdatedAt      int64   `json:"updated_at"`
	CompletedAt    *int64  `json:"completed_at"`
	Version        int     `json:"version"`
}

// WorkflowEvent represents a row in the workflow_events table.
type WorkflowEvent struct {
	EventID        int64   `json:"event_id"`
	WorkflowID     string  `json:"workflow_id"`
	EventType      string  `json:"event_type"`
	PayloadJSON    *string `json:"payload_json,omitempty"`
	SequenceNumber int     `json:"sequence_number"`
	CreatedAt      int64   `json:"created_at"`
}

// WorkflowStep represents a row in the workflow_steps table.
type WorkflowStep struct {
	WorkflowID  string  `json:"workflow_id"`
	StepNumber  int     `json:"step_number"`
	Status      string  `json:"status"`
	InputJSON   *string `json:"input_json,omitempty"`
	OutputJSON  *string `json:"output_json,omitempty"`
	StartedAt   *int64  `json:"started_at,omitempty"`
	CompletedAt *int64  `json:"completed_at,omitempty"`
}

// Signal represents a row in the workflow_signals table.
type Signal struct {
	SignalID    int64   `json:"signal_id"`
	WorkflowID  string  `json:"workflow_id"`
	SignalType  string  `json:"signal_type"`
	PayloadJSON *string `json:"payload_json,omitempty"`
	Consumed    bool    `json:"consumed"`
	CreatedAt   int64   `json:"created_at"`
}

// Timer represents a row in the workflow_timers table.
type Timer struct {
	TimerID    int64  `json:"timer_id"`
	WorkflowID string `json:"workflow_id"`
	WakeAt     int64  `json:"wake_at"`
	Fired      bool   `json:"fired"`
}

// RetryConfig defines the retry behavior for a step.
type RetryConfig struct {
	MaxRetries int         `json:"max_retries"`
	Policy     RetryPolicy `json:"policy"`
}

// RetryPolicy computes the delay before the next retry.
type RetryPolicy interface {
	NextDelay(retryCount int) int64
}

// FixedBackoff returns a constant delay for every retry.
type FixedBackoff struct {
	DelaySeconds int64 `json:"delay_seconds"`
}

func (b FixedBackoff) NextDelay(_ int) int64 {
	return b.DelaySeconds
}

// LinearBackoff increases the delay linearly with each retry.
type LinearBackoff struct {
	BaseSeconds int64 `json:"base_seconds"`
}

func (b LinearBackoff) NextDelay(retryCount int) int64 {
	return b.BaseSeconds * int64(retryCount+1)
}

// ExponentialBackoff doubles the delay with each retry.
type ExponentialBackoff struct {
	BaseSeconds int64 `json:"base_seconds"`
	MaxSeconds  int64 `json:"max_seconds,omitempty"`
}

func (b ExponentialBackoff) NextDelay(retryCount int) int64 {
	delay := b.BaseSeconds
	for i := 0; i < retryCount; i++ {
		delay *= 2
		if b.MaxSeconds > 0 && delay > b.MaxSeconds {
			return b.MaxSeconds
		}
	}
	return delay
}

// jsonString marshals a value to a JSON string, returning nil on error.
func jsonString(v interface{}) *string {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}
