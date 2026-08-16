package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/KantapatSg/goflow-cqrs/internal/adapter/postgres/write"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const projectorName = "read_model_v1"

type Runner struct {
	db       *gorm.DB
	workers  int
	log      *slog.Logger
	instance string
}

func New(db *gorm.DB, workers int, log *slog.Logger) *Runner {
	if workers < 1 {
		workers = 1
	}
	return &Runner{db: db, workers: workers, log: log, instance: "projector-" + uuid.NewString()}
}

func (r *Runner) Run(ctx context.Context) {
	jobs := make(chan write.OutboxEvent, 32)
	var wg sync.WaitGroup
	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for event := range jobs {
				if err := r.apply(ctx, event); err != nil {
					r.log.Error("projection failed", "event", event.EventID, "error", err)
					r.release(event, err)
				}
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		events, err := r.claim(ctx, 16)
		if err != nil {
			r.log.Error("outbox claim failed", "error", err)
			continue
		}
		for _, event := range events {
			select {
			case jobs <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *Runner) claim(ctx context.Context, limit int) ([]write.OutboxEvent, error) {
	var events []write.OutboxEvent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := `SELECT o.* FROM outbox_events o WHERE o.processed_at IS NULL AND o.available_at <= NOW() AND (o.claim_until IS NULL OR o.claim_until < NOW()) AND NOT EXISTS (SELECT 1 FROM outbox_events earlier WHERE earlier.aggregate_type=o.aggregate_type AND earlier.aggregate_id=o.aggregate_id AND earlier.aggregate_version < o.aggregate_version AND earlier.processed_at IS NULL) ORDER BY o.sequence LIMIT ? FOR UPDATE SKIP LOCKED`
		if err := tx.Raw(q, limit).Scan(&events).Error; err != nil {
			return err
		}
		until := time.Now().UTC().Add(30 * time.Second)
		for i := range events {
			if err := tx.Model(&write.OutboxEvent{}).Where("sequence = ?", events[i].Sequence).Updates(map[string]any{"claimed_by": r.instance, "claim_until": until, "attempt_count": events[i].AttemptCount + 1}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return events, err
}

func (r *Runner) release(event write.OutboxEvent, cause error) {
	backoff := time.Second * time.Duration(min(event.AttemptCount+1, 30))
	msg := cause.Error()
	r.db.Model(&write.OutboxEvent{}).Where("sequence = ?", event.Sequence).Updates(map[string]any{"claimed_by": nil, "claim_until": nil, "available_at": time.Now().UTC().Add(backoff), "last_error": msg})
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (r *Runner) apply(ctx context.Context, event write.OutboxEvent) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		receipt := tx.Exec("INSERT INTO goflow_read.projection_receipts(projector_name,event_id,processed_at) VALUES (?,?,?) ON CONFLICT DO NOTHING", projectorName, event.EventID, now)
		if receipt.Error != nil {
			return receipt.Error
		}
		if receipt.RowsAffected == 0 {
			return tx.Model(&write.OutboxEvent{}).Where("sequence=?", event.Sequence).Updates(map[string]any{"processed_at": now, "claimed_by": nil, "claim_until": nil}).Error
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(event.Payload), &p); err != nil {
			return err
		}
		if err := applyEvent(tx, event.EventType, p, now); err != nil {
			return err
		}
		return tx.Model(&write.OutboxEvent{}).Where("sequence=?", event.Sequence).Updates(map[string]any{"processed_at": now, "claimed_by": nil, "claim_until": nil, "last_error": nil}).Error
	})
}

func applyEvent(tx *gorm.DB, typ string, p map[string]any, now time.Time) error {
	id := func(k string) string { return fmt.Sprint(p[k]) }
	text := func(k string) string { return fmt.Sprint(p[k]) }
	number := func(k string) int {
		v, ok := p[k].(float64)
		if !ok {
			return 0
		}
		return int(v)
	}
	switch typ {
	case "ProjectCreated":
		if err := tx.Exec("INSERT INTO goflow_read.project_views(project_id,name,created_at) VALUES (?,?,?) ON CONFLICT (project_id) DO NOTHING", id("project_id"), text("name"), now).Error; err != nil {
			return err
		}
		return tx.Exec("INSERT INTO goflow_read.project_dashboard_views(project_id,updated_at) VALUES (?,?) ON CONFLICT (project_id) DO NOTHING", id("project_id"), now).Error
	case "TaskCreated":
		return tx.Exec("INSERT INTO goflow_read.task_views(task_id,project_id,name,kind,max_attempts,timeout_ms,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT (task_id) DO NOTHING", id("task_id"), id("project_id"), text("name"), text("kind"), number("max_attempts"), number("timeout_ms"), now).Error
	case "JobSubmitted":
		if err := tx.Exec("INSERT INTO goflow_read.job_views(job_id,project_id,task_id,status,attempt_count,created_at) VALUES (?,?,?,?,?,?) ON CONFLICT (job_id) DO NOTHING", id("job_id"), id("project_id"), id("task_id"), text("status"), number("attempt_count"), now).Error; err != nil {
			return err
		}
		if err := tx.Exec("INSERT INTO goflow_read.project_dashboard_views(project_id,updated_at) VALUES (?,?) ON CONFLICT (project_id) DO NOTHING", id("project_id"), now).Error; err != nil {
			return err
		}
		return dashboard(tx, id("project_id"), "QUEUED", 1, now)
	case "JobStarted":
		if err := tx.Exec("UPDATE goflow_read.job_views SET status='RUNNING',attempt_count=?,started_at=? WHERE job_id=?", number("attempt_count"), now, id("job_id")).Error; err != nil {
			return err
		}
		if err := tx.Exec("INSERT INTO goflow_read.job_attempt_views(job_id,attempt_no,status,started_at) VALUES (?,?,?,?) ON CONFLICT (job_id,attempt_no) DO NOTHING", id("job_id"), number("attempt_no"), "RUNNING", now).Error; err != nil {
			return err
		}
		if err := dashboard(tx, id("project_id"), "QUEUED", -1, now); err != nil {
			return err
		}
		return dashboard(tx, id("project_id"), "RUNNING", 1, now)
	case "JobCompleted", "JobFailed":
		status := text("new_status")
		failure := text("failure")
		if err := tx.Exec("UPDATE goflow_read.job_views SET status=?,last_error=?,completed_at=? WHERE job_id=?", status, nullString(failure), now, id("job_id")).Error; err != nil {
			return err
		}
		if err := tx.Exec("UPDATE goflow_read.job_attempt_views SET status=?,failure=?,completed_at=? WHERE job_id=? AND attempt_no=(SELECT attempt_count FROM goflow_read.job_views WHERE job_id=?)", status, nullString(failure), now, id("job_id"), id("job_id")).Error; err != nil {
			return err
		}
		if err := dashboard(tx, id("project_id"), "RUNNING", -1, now); err != nil {
			return err
		}
		return dashboard(tx, id("project_id"), status, 1, now)
	case "JobRetryScheduled":
		if err := tx.Exec("UPDATE goflow_read.job_views SET status='RETRYING' WHERE job_id=?", id("job_id")).Error; err != nil {
			return err
		}
		if err := dashboard(tx, id("project_id"), "FAILED", -1, now); err != nil {
			return err
		}
		return dashboard(tx, id("project_id"), "QUEUED", 1, now)
	case "JobCancelled":
		if err := tx.Exec("UPDATE goflow_read.job_views SET status='CANCELLED',completed_at=? WHERE job_id=?", now, id("job_id")).Error; err != nil {
			return err
		}
		if err := dashboard(tx, id("project_id"), text("previous_status"), -1, now); err != nil {
			return err
		}
		return dashboard(tx, id("project_id"), "CANCELLED", 1, now)
	}
	return fmt.Errorf("unsupported event %s", typ)
}
func nullString(v string) any {
	if v == "<nil>" || v == "" {
		return nil
	}
	return v
}
func dashboard(tx *gorm.DB, projectID, status string, delta int, now time.Time) error {
	field := map[string]string{"QUEUED": "queued_count", "RUNNING": "running_count", "SUCCESS": "success_count", "FAILED": "failed_count", "CANCELLED": "cancelled_count"}[status]
	if field == "" {
		return nil
	}
	return tx.Exec("UPDATE goflow_read.project_dashboard_views SET "+field+" = "+field+" + ?, updated_at=? WHERE project_id=?", delta, now, projectID).Error
}
