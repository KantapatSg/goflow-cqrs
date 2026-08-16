package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/KantapatSg/goflow-cqrs/internal/adapter/httpapi"
	read "github.com/KantapatSg/goflow-cqrs/internal/adapter/postgres/read"
	"github.com/KantapatSg/goflow-cqrs/internal/adapter/postgres/write"
	"github.com/KantapatSg/goflow-cqrs/internal/app/command"
	"github.com/KantapatSg/goflow-cqrs/internal/projection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	dsn := env("DATABASE_URL", "host=localhost user=goflow password=goflow dbname=goflow port=5432 sslmode=disable")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Error("database pool unavailable", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()
	commands := command.New(write.New(db))
	queries := read.New(sqlDB)
	router := httpapi.New(commands, queries)
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner := projection.New(db, envInt("PROJECTOR_WORKERS", 2), log)
	done := make(chan struct{})
	go func() { defer close(done); runner.Run(root) }()
	server := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: router, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("api listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()
	<-root.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Error("http shutdown failed", "error", err)
	}
	<-done
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(env(key, ""))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}
