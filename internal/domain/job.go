package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTransition = errors.New("invalid job state transition")
	ErrAttemptsExhausted = errors.New("job attempts exhausted")
	ErrAttemptNotActive  = errors.New("job attempt is not active")
)

type JobStatus string

const (
	Queued    JobStatus = "QUEUED"
	Running   JobStatus = "RUNNING"
	Success   JobStatus = "SUCCESS"
	Failed    JobStatus = "FAILED"
	Retrying  JobStatus = "RETRYING"
	Cancelled JobStatus = "CANCELLED"
)

type Project struct {
	ID        uuid.UUID
	Name      string
	Version   int64
	CreatedAt time.Time
}
type Task struct {
	ID, ProjectID uuid.UUID
	Name, Kind    string
	MaxAttempts   int
	Timeout       time.Duration
	Version       int64
	CreatedAt     time.Time
}
type JobAttempt struct {
	ID, JobID              uuid.UUID
	Number                 int
	Status                 JobStatus
	WorkerID, Failure      string
	StartedAt, CompletedAt *time.Time
}
type Event struct {
	ID               uuid.UUID
	Type             string
	AggregateType    string
	AggregateID      uuid.UUID
	AggregateVersion int64
	OccurredAt       time.Time
	Payload          map[string]any
}

type Job struct {
	ID, ProjectID, TaskID     uuid.UUID
	Status                    JobStatus
	Version                   int64
	AttemptCount, MaxAttempts int
	ActiveAttemptID           *uuid.UUID
}

func NewJob(id, projectID, taskID uuid.UUID, maxAttempts int) (Job, error) {
	if maxAttempts < 1 {
		return Job{}, fmt.Errorf("max attempts: %w", ErrAttemptsExhausted)
	}
	return Job{ID: id, ProjectID: projectID, TaskID: taskID, Status: Queued, Version: 1, MaxAttempts: maxAttempts}, nil
}

func (j *Job) Start(attemptID uuid.UUID, workerID string, now time.Time) (JobAttempt, Event, error) {
	if j.Status != Queued && j.Status != Retrying {
		return JobAttempt{}, Event{}, ErrInvalidTransition
	}
	if j.AttemptCount >= j.MaxAttempts {
		return JobAttempt{}, Event{}, ErrAttemptsExhausted
	}
	j.AttemptCount++
	j.Status = Running
	j.Version++
	j.ActiveAttemptID = &attemptID
	a := JobAttempt{ID: attemptID, JobID: j.ID, Number: j.AttemptCount, Status: Running, WorkerID: workerID, StartedAt: &now}
	return a, j.event("JobStarted", now, map[string]any{"attempt_id": attemptID, "attempt_no": a.Number, "worker_id": workerID, "previous_status": Queued, "new_status": Running}), nil
}

func (j *Job) Complete(attemptID uuid.UUID, now time.Time) (Event, error) {
	if j.Status != Running || j.ActiveAttemptID == nil || *j.ActiveAttemptID != attemptID {
		return Event{}, ErrAttemptNotActive
	}
	j.Status = Success
	j.Version++
	j.ActiveAttemptID = nil
	return j.event("JobCompleted", now, map[string]any{"attempt_id": attemptID, "previous_status": Running, "new_status": Success}), nil
}

func (j *Job) Fail(attemptID uuid.UUID, failure string, now time.Time) (Event, error) {
	if j.Status != Running || j.ActiveAttemptID == nil || *j.ActiveAttemptID != attemptID {
		return Event{}, ErrAttemptNotActive
	}
	j.Status = Failed
	j.Version++
	j.ActiveAttemptID = nil
	return j.event("JobFailed", now, map[string]any{"attempt_id": attemptID, "failure": failure, "previous_status": Running, "new_status": Failed}), nil
}

func (j *Job) Retry(now time.Time) (Event, error) {
	if j.Status != Failed {
		return Event{}, ErrInvalidTransition
	}
	if j.AttemptCount >= j.MaxAttempts {
		return Event{}, ErrAttemptsExhausted
	}
	j.Status = Retrying
	j.Version++
	return j.event("JobRetryScheduled", now, map[string]any{"previous_status": Failed, "new_status": Retrying}), nil
}

func (j *Job) Cancel(now time.Time) (Event, error) {
	if j.Status != Queued && j.Status != Retrying && j.Status != Failed {
		return Event{}, ErrInvalidTransition
	}
	previous := j.Status
	j.Status = Cancelled
	j.Version++
	return j.event("JobCancelled", now, map[string]any{"previous_status": previous, "new_status": Cancelled}), nil
}

func (j Job) event(typ string, now time.Time, extra map[string]any) Event {
	p := map[string]any{"job_id": j.ID, "project_id": j.ProjectID, "task_id": j.TaskID, "status": j.Status, "attempt_count": j.AttemptCount}
	for k, v := range extra {
		p[k] = v
	}
	return Event{ID: uuid.New(), Type: typ, AggregateType: "job", AggregateID: j.ID, AggregateVersion: j.Version, OccurredAt: now, Payload: p}
}
