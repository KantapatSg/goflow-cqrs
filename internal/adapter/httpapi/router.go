package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	read "github.com/KantapatSg/goflow-cqrs/internal/adapter/postgres/read"
	"github.com/KantapatSg/goflow-cqrs/internal/app/command"
	"github.com/KantapatSg/goflow-cqrs/internal/domain"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func New(commands *command.Service, queries *read.Repository) *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	v1 := r.Group("/v1")
	v1.POST("/projects", func(c *gin.Context) {
		var in struct {
			Name string `json:"name"`
		}
		if !bind(c, &in) {
			return
		}
		p, err := commands.CreateProject(c, in.Name)
		respond(c, p, err)
	})
	v1.POST("/projects/:projectID/tasks", func(c *gin.Context) {
		var in struct {
			Name        string `json:"name"`
			Kind        string `json:"kind"`
			MaxAttempts int    `json:"maxAttempts"`
			TimeoutMS   int64  `json:"timeoutMs"`
		}
		if !bind(c, &in) {
			return
		}
		id, ok := pathID(c, "projectID")
		if !ok {
			return
		}
		t, err := commands.CreateTask(c, id, in.Name, in.Kind, in.MaxAttempts, time.Duration(in.TimeoutMS)*time.Millisecond)
		respond(c, t, err)
	})
	v1.POST("/tasks/:taskID/jobs", func(c *gin.Context) {
		var in struct {
			IdempotencyKey string          `json:"idempotencyKey"`
			Input          json.RawMessage `json:"input"`
		}
		if !bind(c, &in) {
			return
		}
		id, ok := pathID(c, "taskID")
		if !ok {
			return
		}
		j, err := commands.SubmitJob(c, id, in.IdempotencyKey, string(in.Input))
		respond(c, j, err)
	})
	v1.POST("/jobs/:jobID/start", func(c *gin.Context) {
		var in struct {
			WorkerID string `json:"workerId"`
		}
		if !bind(c, &in) {
			return
		}
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		a, j, err := commands.StartJob(c, id, in.WorkerID)
		if err != nil {
			respond(c, nil, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"job": j, "attempt": a})
	})
	v1.POST("/jobs/:jobID/complete", func(c *gin.Context) {
		var in struct {
			AttemptID string `json:"attemptId"`
		}
		if !bind(c, &in) {
			return
		}
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		a, err := uuid.Parse(in.AttemptID)
		if err != nil {
			bad(c, "attemptId must be UUID")
			return
		}
		j, err := commands.CompleteJob(c, id, a)
		respond(c, j, err)
	})
	v1.POST("/jobs/:jobID/fail", func(c *gin.Context) {
		var in struct {
			AttemptID string `json:"attemptId"`
			Failure   string `json:"failure"`
		}
		if !bind(c, &in) {
			return
		}
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		a, err := uuid.Parse(in.AttemptID)
		if err != nil {
			bad(c, "attemptId must be UUID")
			return
		}
		j, err := commands.FailJob(c, id, a, in.Failure)
		respond(c, j, err)
	})
	v1.POST("/jobs/:jobID/retry", func(c *gin.Context) {
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		j, err := commands.RetryJob(c, id)
		respond(c, j, err)
	})
	v1.POST("/jobs/:jobID/cancel", func(c *gin.Context) {
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		j, err := commands.CancelJob(c, id)
		respond(c, j, err)
	})
	v1.GET("/projects/:projectID", func(c *gin.Context) {
		id, ok := pathID(c, "projectID")
		if !ok {
			return
		}
		v, err := queries.Project(c, id)
		queryRespond(c, v, err)
	})
	v1.GET("/projects/:projectID/dashboard", func(c *gin.Context) {
		id, ok := pathID(c, "projectID")
		if !ok {
			return
		}
		v, err := queries.Dashboard(c, id)
		queryRespond(c, v, err)
	})
	v1.GET("/tasks/:taskID", func(c *gin.Context) {
		id, ok := pathID(c, "taskID")
		if !ok {
			return
		}
		v, err := queries.Task(c, id)
		queryRespond(c, v, err)
	})
	v1.GET("/jobs/:jobID", func(c *gin.Context) {
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		v, err := queries.Job(c, id)
		queryRespond(c, v, err)
	})
	v1.GET("/jobs/:jobID/history", func(c *gin.Context) {
		id, ok := pathID(c, "jobID")
		if !ok {
			return
		}
		v, err := queries.History(c, id)
		queryRespond(c, v, err)
	})
	v1.GET("/jobs", func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		if limit < 1 || limit > 100 {
			bad(c, "limit must be 1..100")
			return
		}
		var pid *uuid.UUID
		if raw := c.Query("projectId"); raw != "" {
			p, err := uuid.Parse(raw)
			if err != nil {
				bad(c, "projectId must be UUID")
				return
			}
			pid = &p
		}
		v, err := queries.Jobs(c, pid, c.Query("status"), limit)
		queryRespond(c, v, err)
	})
	return r
}
func bind(c *gin.Context, in any) bool {
	if err := c.ShouldBindJSON(in); err != nil {
		bad(c, err.Error())
		return false
	}
	return true
}
func pathID(c *gin.Context, key string) (uuid.UUID, bool) {
	id, err := command.ParseID(c.Param(key))
	if err != nil {
		bad(c, err.Error())
		return uuid.Nil, false
	}
	return id, true
}
func bad(c *gin.Context, msg string) { c.JSON(http.StatusBadRequest, gin.H{"error": msg}) }
func respond(c *gin.Context, v any, err error) {
	if err == nil {
		c.JSON(http.StatusCreated, v)
		return
	}
	if errors.Is(err, command.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if errors.Is(err, domain.ErrInvalidTransition) || errors.Is(err, domain.ErrAttemptNotActive) || errors.Is(err, domain.ErrAttemptsExhausted) {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	bad(c, err.Error())
}
func queryRespond(c *gin.Context, v any, err error) {
	if err == nil {
		c.JSON(http.StatusOK, v)
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "read model not ready or record not found"})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
}
