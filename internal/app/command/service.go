package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KantapatSg/goflow-cqrs/internal/domain"
	"github.com/google/uuid"
)

var ErrNotFound = errors.New("record not found")

type Repository interface {
	CreateProject(context.Context, domain.Project, domain.Event) error
	CreateTask(context.Context, domain.Task, domain.Event) error
	GetTask(context.Context, uuid.UUID) (domain.Task, error)
	CreateJob(context.Context, domain.Job, string, string, domain.Event) error
	GetJob(context.Context, uuid.UUID) (domain.Job, error)
	SaveJob(context.Context, domain.Job, domain.Event, *domain.JobAttempt) error
}

type Service struct {
	repo Repository
	now  func() time.Time
}

func New(repo Repository) *Service {
	return &Service{repo: repo, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) CreateProject(ctx context.Context, name string) (domain.Project, error) {
	if name == "" {
		return domain.Project{}, errors.New("project name is required")
	}
	now := s.now()
	p := domain.Project{ID: uuid.New(), Name: name, Version: 1, CreatedAt: now}
	e := domain.Event{ID: uuid.New(), Type: "ProjectCreated", AggregateType: "project", AggregateID: p.ID, AggregateVersion: 1, OccurredAt: now, Payload: map[string]any{"project_id": p.ID, "name": p.Name, "created_at": now}}
	return p, s.repo.CreateProject(ctx, p, e)
}

func (s *Service) CreateTask(ctx context.Context, projectID uuid.UUID, name, kind string, maxAttempts int, timeout time.Duration) (domain.Task, error) {
	if name == "" || kind == "" || maxAttempts < 1 || timeout <= 0 {
		return domain.Task{}, errors.New("invalid task input")
	}
	now := s.now()
	t := domain.Task{ID: uuid.New(), ProjectID: projectID, Name: name, Kind: kind, MaxAttempts: maxAttempts, Timeout: timeout, Version: 1, CreatedAt: now}
	e := domain.Event{ID: uuid.New(), Type: "TaskCreated", AggregateType: "task", AggregateID: t.ID, AggregateVersion: 1, OccurredAt: now, Payload: map[string]any{"task_id": t.ID, "project_id": projectID, "name": name, "kind": kind, "max_attempts": maxAttempts, "timeout_ms": timeout.Milliseconds(), "created_at": now}}
	return t, s.repo.CreateTask(ctx, t, e)
}

func (s *Service) SubmitJob(ctx context.Context, taskID uuid.UUID, idempotencyKey, input string) (domain.Job, error) {
	if input == "" {
		input = "{}"
	}
	if !json.Valid([]byte(input)) {
		return domain.Job{}, errors.New("job input must be valid JSON")
	}
	t, err := s.repo.GetTask(ctx, taskID)
	if err != nil {
		return domain.Job{}, err
	}
	j, err := domain.NewJob(uuid.New(), t.ProjectID, t.ID, t.MaxAttempts)
	if err != nil {
		return domain.Job{}, err
	}
	now := s.now()
	e := domain.Event{ID: uuid.New(), Type: "JobSubmitted", AggregateType: "job", AggregateID: j.ID, AggregateVersion: j.Version, OccurredAt: now, Payload: map[string]any{"job_id": j.ID, "project_id": t.ProjectID, "task_id": t.ID, "status": j.Status, "attempt_count": 0, "created_at": now}}
	return j, s.repo.CreateJob(ctx, j, idempotencyKey, input, e)
}

func (s *Service) StartJob(ctx context.Context, id uuid.UUID, workerID string) (domain.JobAttempt, domain.Job, error) {
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.JobAttempt{}, domain.Job{}, err
	}
	a, e, err := j.Start(uuid.New(), workerID, s.now())
	if err != nil {
		return domain.JobAttempt{}, domain.Job{}, err
	}
	return a, j, s.repo.SaveJob(ctx, j, e, &a)
}
func (s *Service) CompleteJob(ctx context.Context, id, attemptID uuid.UUID) (domain.Job, error) {
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	e, err := j.Complete(attemptID, s.now())
	if err != nil {
		return domain.Job{}, err
	}
	return j, s.repo.SaveJob(ctx, j, e, nil)
}
func (s *Service) FailJob(ctx context.Context, id, attemptID uuid.UUID, failure string) (domain.Job, error) {
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	e, err := j.Fail(attemptID, failure, s.now())
	if err != nil {
		return domain.Job{}, err
	}
	return j, s.repo.SaveJob(ctx, j, e, nil)
}
func (s *Service) RetryJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	e, err := j.Retry(s.now())
	if err != nil {
		return domain.Job{}, err
	}
	return j, s.repo.SaveJob(ctx, j, e, nil)
}
func (s *Service) CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	e, err := j.Cancel(s.now())
	if err != nil {
		return domain.Job{}, err
	}
	return j, s.repo.SaveJob(ctx, j, e, nil)
}

func ParseID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid UUID: %w", err)
	}
	return id, nil
}
