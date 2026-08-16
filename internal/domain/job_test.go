package domain

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestJobStateMachine(t *testing.T) {
	now := time.Now()
	j, _ := NewJob(uuid.New(), uuid.New(), uuid.New(), 2)
	a, e, err := j.Start(uuid.New(), "worker-1", now)
	if err != nil || e.Type != "JobStarted" || j.Status != Running {
		t.Fatalf("start: %v, %s", err, j.Status)
	}
	if _, err = j.Complete(a.ID, now); err != nil || j.Status != Success {
		t.Fatalf("complete: %v, %s", err, j.Status)
	}
	if _, _, err = j.Start(uuid.New(), "worker-2", now); err != ErrInvalidTransition {
		t.Fatalf("want invalid transition, got %v", err)
	}
}
