package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ezhigval/go-toolkit/httputil"
	"github.com/ezhigval/go-toolkit/logger"
	tkmw "github.com/ezhigval/go-toolkit/middleware"
	tkpgx "github.com/ezhigval/go-toolkit/pgx"
	tkredis "github.com/ezhigval/go-toolkit/redis"
	"github.com/ezhigval/notification-hub/internal/channel"
	"github.com/ezhigval/notification-hub/internal/config"
	"github.com/ezhigval/notification-hub/internal/dedup"
	"github.com/ezhigval/notification-hub/internal/handler"
	hubkafka "github.com/ezhigval/notification-hub/internal/kafka"
	"github.com/ezhigval/notification-hub/internal/repository"
	"github.com/ezhigval/notification-hub/internal/service"
	"github.com/ezhigval/notification-hub/internal/template"
	"github.com/ezhigval/notification-hub/internal/worker"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
)

func main() {
	cfg := config.MustLoad()
	log := logger.New(logger.Config{Level: cfg.LogLevel, Format: cfg.LogFormat})
	ctx := context.Background()

	pool, err := tkpgx.NewPool(ctx, tkpgx.Config{URL: cfg.DatabaseURL, MaxConns: 20})
	if err != nil {
		log.Error("postgres failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := tkredis.NewClient(tkredis.Config{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer func() { _ = tkredis.Close(rdb) }()

	if err := tkpgx.Ping(ctx, pool); err != nil {
		log.Error("postgres ping failed", "error", err)
		os.Exit(1)
	}
	if err := tkredis.Ping(ctx, rdb); err != nil {
		log.Error("redis ping failed", "error", err)
		os.Exit(1)
	}

	topics := []string{cfg.KafkaTopicHigh, cfg.KafkaTopicNormal, cfg.KafkaTopicLow, cfg.KafkaTopicDLQ}
	publisher := hubkafka.NewPublisher(cfg.KafkaBrokers, topics...)
	defer func() { _ = publisher.Close() }()

	templates := repository.NewTemplateRepository(pool)
	notifs := repository.NewNotificationRepository(pool)
	dedupStore := dedup.NewStore(rdb)
	engine := template.NewEngine()
	channels := channel.NewRegistry(log)

	svc := service.NewNotificationService(cfg, templates, notifs, dedupStore, publisher, engine, channels, log)
	h := handler.New(svc)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	if cfg.EnableWorker {
		consumers := hubkafka.NewConsumerPool(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, []string{
			cfg.KafkaTopicHigh, cfg.KafkaTopicNormal, cfg.KafkaTopicLow,
		})
		defer func() { _ = consumers.Close() }()
		wp := worker.NewPool(cfg, svc, consumers, log)
		go wp.Run(workerCtx)
	}

	r := chi.NewRouter()
	r.Use(tkmw.RequestID, tkmw.RealIP, tkmw.Recoverer(log), tkmw.AccessLog(log))
	r.Use(chimw.Timeout(30 * time.Second))

	r.Get("/health", httputil.HealthHandler(map[string]func() error{
		"postgres": func() error { return tkpgx.Ping(ctx, pool) },
		"redis":    func() error { return tkredis.Ping(ctx, rdb) },
	}))

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/templates", h.ListTemplates)
		r.Post("/templates", h.CreateTemplate)
		r.Post("/notifications", h.Send)
		r.Get("/notifications/{id}", h.GetNotification)
		r.Get("/notifications/{id}/attempts", h.ListAttempts)
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		log.Info("notification-hub started", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	workerCancel()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("server stopped")
}
