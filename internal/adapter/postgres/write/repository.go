package write

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/KantapatSg/goflow-cqrs/internal/app/command"
	"github.com/KantapatSg/goflow-cqrs/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type ProjectModel struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey"`
	Name      string
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (ProjectModel) TableName() string { return "projects" }

type TaskModel struct {
	ID                   uuid.UUID `gorm:"type:uuid;primaryKey"`
	ProjectID            uuid.UUID `gorm:"type:uuid"`
	Name, Kind           string
	MaxAttempts          int
	TimeoutMS            int64
	Version              int64
	CreatedAt, UpdatedAt time.Time
}

func (TaskModel) TableName() string { return "tasks" }

type JobModel struct {
	ID, ProjectID, TaskID     uuid.UUID `gorm:"type:uuid"`
	Status                    string
	Version                   int64
	AttemptCount, MaxAttempts int
	ActiveAttemptID           *uuid.UUID `gorm:"type:uuid"`
	IdempotencyKey            *string
	Input                     string `gorm:"type:jsonb"`
	CreatedAt, UpdatedAt      time.Time
}

func (JobModel) TableName() string { return "jobs" }

type AttemptModel struct {
	ID, JobID                 uuid.UUID `gorm:"type:uuid"`
	AttemptNo                 int
	Status, WorkerID, Failure string
	StartedAt, CompletedAt    *time.Time
}

func (AttemptModel) TableName() string { return "job_attempts" }

type OutboxEvent struct {
	Sequence                int64
	EventID                 uuid.UUID `gorm:"type:uuid"`
	EventType               string
	SchemaVersion           int
	AggregateType           string
	AggregateID             uuid.UUID `gorm:"type:uuid"`
	AggregateVersion        int64
	OccurredAt, AvailableAt time.Time
	Payload                 string `gorm:"type:jsonb"`
	ClaimedBy               *string
	ClaimUntil              *time.Time
	AttemptCount            int
	ProcessedAt             *time.Time
	LastError               *string
}

func (OutboxEvent) TableName() string { return "outbox_events" }

type Repository struct{ db *gorm.DB }

func New(db *gorm.DB) *Repository { return &Repository{db: db} }
func eventModel(e domain.Event) OutboxEvent {
	raw, _ := json.Marshal(e.Payload)
	return OutboxEvent{EventID: e.ID, EventType: e.Type, SchemaVersion: 1, AggregateType: e.AggregateType, AggregateID: e.AggregateID, AggregateVersion: e.AggregateVersion, OccurredAt: e.OccurredAt, AvailableAt: e.OccurredAt, Payload: string(raw)}
}
func createEvent(tx *gorm.DB, e domain.Event) error { m := eventModel(e); return tx.Create(&m).Error }

func (r *Repository) CreateProject(ctx context.Context, p domain.Project, e domain.Event) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := e.OccurredAt
		if err := tx.Create(&ProjectModel{ID: p.ID, Name: p.Name, Version: p.Version, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
			return err
		}
		return createEvent(tx, e)
	})
}
func (r *Repository) CreateTask(ctx context.Context, t domain.Task, e domain.Event) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		tx.Model(&ProjectModel{}).Where("id = ?", t.ProjectID).Count(&count)
		if count == 0 {
			return command.ErrNotFound
		}
		now := e.OccurredAt
		if err := tx.Create(&TaskModel{ID: t.ID, ProjectID: t.ProjectID, Name: t.Name, Kind: t.Kind, MaxAttempts: t.MaxAttempts, TimeoutMS: t.Timeout.Milliseconds(), Version: t.Version, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
			return err
		}
		return createEvent(tx, e)
	})
}
func (r *Repository) GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	var m TaskModel
	if err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.Task{}, command.ErrNotFound
		}
		return domain.Task{}, err
	}
	return domain.Task{ID: m.ID, ProjectID: m.ProjectID, Name: m.Name, Kind: m.Kind, MaxAttempts: m.MaxAttempts, Timeout: time.Duration(m.TimeoutMS) * time.Millisecond, Version: m.Version, CreatedAt: m.CreatedAt}, nil
}
func (r *Repository) CreateJob(ctx context.Context, j domain.Job, key, input string, e domain.Event) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := e.OccurredAt
		var keyPtr *string
		if key != "" {
			keyPtr = &key
		}
		if input == "" {
			input = "{}"
		}
		m := JobModel{ID: j.ID, ProjectID: j.ProjectID, TaskID: j.TaskID, Status: string(j.Status), Version: j.Version, AttemptCount: j.AttemptCount, MaxAttempts: j.MaxAttempts, IdempotencyKey: keyPtr, Input: input, CreatedAt: now, UpdatedAt: now}
		if err := tx.Create(&m).Error; err != nil {
			return err
		}
		return createEvent(tx, e)
	})
}
func (r *Repository) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	var m JobModel
	if err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.Job{}, command.ErrNotFound
		}
		return domain.Job{}, err
	}
	return domain.Job{ID: m.ID, ProjectID: m.ProjectID, TaskID: m.TaskID, Status: domain.JobStatus(m.Status), Version: m.Version, AttemptCount: m.AttemptCount, MaxAttempts: m.MaxAttempts, ActiveAttemptID: m.ActiveAttemptID}, nil
}
func (r *Repository) SaveJob(ctx context.Context, j domain.Job, e domain.Event, a *domain.JobAttempt) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&JobModel{}).Where("id = ? AND version = ?", j.ID, j.Version-1).Updates(map[string]any{"status": j.Status, "version": j.Version, "attempt_count": j.AttemptCount, "active_attempt_id": j.ActiveAttemptID, "updated_at": e.OccurredAt})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return domain.ErrInvalidTransition
		}
		if a != nil {
			if err := tx.Create(&AttemptModel{ID: a.ID, JobID: a.JobID, AttemptNo: a.Number, Status: string(a.Status), WorkerID: a.WorkerID, StartedAt: a.StartedAt}).Error; err != nil {
				return err
			}
		} else if e.Type == "JobCompleted" || e.Type == "JobFailed" {
			attemptID, ok := e.Payload["attempt_id"].(uuid.UUID)
			if !ok {
				return errors.New("event attempt_id is invalid")
			}
			updates := map[string]any{"status": string(j.Status), "completed_at": e.OccurredAt}
			if e.Type == "JobFailed" {
				updates["failure"] = e.Payload["failure"]
			}
			{
				if err := tx.Model(&AttemptModel{}).Where("id = ?", attemptID).Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		return createEvent(tx, e)
	})
}
