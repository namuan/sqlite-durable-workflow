package durable

import (
	"fmt"
	"time"
)

// DefaultRetryConfig provides a sensible retry default: 3 retries with
// exponential backoff starting at 1 second.
var DefaultRetryConfig = RetryConfig{
	MaxRetries: 3,
	Policy: ExponentialBackoff{
		BaseSeconds: 1,
		MaxSeconds:  60,
	},
}

// ScheduleRetry computes the next retry time for a failed step. If
// retryCount exceeds MaxRetries, returns (0, false) indicating that
// the workflow should be failed permanently. Otherwise returns the
// epoch second at which the retry should occur.
func (c RetryConfig) ScheduleRetry(retryCount int) (int64, bool) {
	if retryCount > c.MaxRetries {
		return 0, false
	}
	delay := c.Policy.NextDelay(retryCount)
	wakeAt := time.Now().Unix() + delay
	return wakeAt, true
}

// RetryStep schedules a retry for a failed step. It creates a timer
// for the retry delay and marks the workflow as WAITING. When the
// timer fires, the scheduler will re-queue the workflow. Returns
// (false, nil) if the step has exceeded max retries (caller should
// fail the workflow).
func (e *Engine) RetryStep(workflowID, workerID string, stepNumber int, retryCount int, cfg RetryConfig) (bool, error) {
	wakeAt, ok := cfg.ScheduleRetry(retryCount)
	if !ok {
		return false, nil
	}

	wakeTime := time.Unix(wakeAt, 0)
	if _, err := e.CreateTimer(workflowID, wakeTime); err != nil {
		return false, fmt.Errorf("create retry timer: %w", err)
	}

	if err := e.MarkWaiting(workflowID, workerID); err != nil {
		return false, fmt.Errorf("mark waiting: %w", err)
	}

	return true, nil
}
