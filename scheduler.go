package durable

import (
	"context"
	"log"
	"time"
)

// DefaultPollInterval is the interval at which the scheduler checks
// for expired timers and leases.
const DefaultPollInterval = 1 * time.Second

// Scheduler is a lightweight, stateless background loop that performs
// periodic housekeeping: firing expired timers, expiring stale leases,
// and requeuing abandoned workflows.
type Scheduler struct {
	engine   *Engine
	interval time.Duration
}

// NewScheduler creates a scheduler that runs against the given engine
// at the specified poll interval.
func NewScheduler(engine *Engine, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	return &Scheduler{
		engine:   engine,
		interval: interval,
	}
}

// Run starts the scheduler loop. It blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Printf("[scheduler] started (interval=%s)", s.interval)

	for {
		s.tick()

		select {
		case <-ctx.Done():
			log.Printf("[scheduler] stopped")
			return
		case <-ticker.C:
		}
	}
}

// Tick performs a single scheduler cycle: firing timers then expiring
// leases. Exported for manual stepping in tests.
func (s *Scheduler) Tick() {
	s.tick()
}

func (s *Scheduler) tick() {
	now := time.Now().Unix()

	if n, err := s.engine.fireExpiredTimers(now); err != nil {
		log.Printf("[scheduler] fire timers error: %v", err)
	} else if n > 0 {
		log.Printf("[scheduler] fired %d timer(s)", n)
	}

	if n, err := s.engine.expireLeases(now); err != nil {
		log.Printf("[scheduler] expire leases error: %v", err)
	} else if n > 0 {
		log.Printf("[scheduler] expired %d lease(s)", n)
	}
}
