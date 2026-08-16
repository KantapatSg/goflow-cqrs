package read

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"time"
)

type Project struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}
type Task struct {
	ID, ProjectID uuid.UUID
	Name, Kind    string
	MaxAttempts   int
	TimeoutMS     int64
	CreatedAt     time.Time
}
type Job struct {
	ID, ProjectID, TaskID  uuid.UUID
	Status                 string
	AttemptCount           int
	LastError              sql.NullString
	CreatedAt              time.Time
	StartedAt, CompletedAt sql.NullTime
}
type Attempt struct {
	Number                 int
	Status                 string
	Failure                sql.NullString
	StartedAt, CompletedAt sql.NullTime
}
type Dashboard struct {
	ProjectID                                   uuid.UUID
	Queued, Running, Success, Failed, Cancelled int64
	UpdatedAt                                   time.Time
}
type Repository struct{ db *sql.DB }

func New(db *sql.DB) *Repository { return &Repository{db: db} }
func (r *Repository) Project(ctx context.Context, id uuid.UUID) (Project, error) {
	var v Project
	err := r.db.QueryRowContext(ctx, "SELECT project_id,name,created_at FROM goflow_read.project_views WHERE project_id=$1", id).Scan(&v.ID, &v.Name, &v.CreatedAt)
	return v, err
}
func (r *Repository) Task(ctx context.Context, id uuid.UUID) (Task, error) {
	var v Task
	err := r.db.QueryRowContext(ctx, "SELECT task_id,project_id,name,kind,max_attempts,timeout_ms,created_at FROM goflow_read.task_views WHERE task_id=$1", id).Scan(&v.ID, &v.ProjectID, &v.Name, &v.Kind, &v.MaxAttempts, &v.TimeoutMS, &v.CreatedAt)
	return v, err
}
func (r *Repository) Job(ctx context.Context, id uuid.UUID) (Job, error) {
	var v Job
	err := r.db.QueryRowContext(ctx, "SELECT job_id,project_id,task_id,status,attempt_count,last_error,created_at,started_at,completed_at FROM goflow_read.job_views WHERE job_id=$1", id).Scan(&v.ID, &v.ProjectID, &v.TaskID, &v.Status, &v.AttemptCount, &v.LastError, &v.CreatedAt, &v.StartedAt, &v.CompletedAt)
	return v, err
}
func (r *Repository) Jobs(ctx context.Context, projectID *uuid.UUID, status string, limit int) ([]Job, error) {
	q := "SELECT job_id,project_id,task_id,status,attempt_count,last_error,created_at,started_at,completed_at FROM goflow_read.job_views WHERE 1=1"
	args := []any{}
	if projectID != nil {
		args = append(args, *projectID)
		q += " AND project_id=$" + itoa(len(args))
	}
	if status != "" {
		args = append(args, status)
		q += " AND status=$" + itoa(len(args))
	}
	args = append(args, limit)
	q += " ORDER BY created_at DESC,job_id DESC LIMIT $" + itoa(len(args))
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		var v Job
		if err := rows.Scan(&v.ID, &v.ProjectID, &v.TaskID, &v.Status, &v.AttemptCount, &v.LastError, &v.CreatedAt, &v.StartedAt, &v.CompletedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (r *Repository) History(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT attempt_no,status,failure,started_at,completed_at FROM goflow_read.job_attempt_views WHERE job_id=$1 ORDER BY attempt_no", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		var v Attempt
		if err := rows.Scan(&v.Number, &v.Status, &v.Failure, &v.StartedAt, &v.CompletedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (r *Repository) Dashboard(ctx context.Context, id uuid.UUID) (Dashboard, error) {
	var v Dashboard
	err := r.db.QueryRowContext(ctx, "SELECT project_id,queued_count,running_count,success_count,failed_count,cancelled_count,updated_at FROM goflow_read.project_dashboard_views WHERE project_id=$1", id).Scan(&v.ProjectID, &v.Queued, &v.Running, &v.Success, &v.Failed, &v.Cancelled, &v.UpdatedAt)
	return v, err
}
func itoa(n int) string { return string(rune('0' + n)) }
