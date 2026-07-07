package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ezhigval/notification-hub/internal/config"
	"github.com/ezhigval/notification-hub/internal/kafka"
	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/ezhigval/notification-hub/internal/service"
	kafkago "github.com/segmentio/kafka-go"
)

type Pool struct {
	cfg  config.Config
	svc  *service.NotificationService
	pool *kafka.ConsumerPool
	log  *slog.Logger
}

func NewPool(cfg config.Config, svc *service.NotificationService, pool *kafka.ConsumerPool, log *slog.Logger) *Pool {
	return &Pool{
		cfg:  cfg,
		svc:  svc,
		pool: pool,
		log:  log,
	}
}

func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, reader := range p.pool.Readers() {
		wg.Add(1)
		go func(r *kafkago.Reader) {
			defer wg.Done()
			p.consume(ctx, r)
		}(reader)
	}

	retryTicker := time.NewTicker(p.cfg.RetryPollInterval)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-retryTicker.C:
			if err := p.svc.RepublishDueRetries(ctx); err != nil {
				p.log.Error("retry poller failed", "error", err)
			}
		}
	}
}

func (p *Pool) consume(ctx context.Context, reader *kafkago.Reader) {
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("kafka read failed", "error", err, "topic", reader.Config().Topic)
			time.Sleep(time.Second)
			continue
		}
		p.handle(ctx, reader.Config().Topic, msg.Value)
	}
}

func (p *Pool) handle(ctx context.Context, topic string, raw []byte) {
	if topic == p.cfg.KafkaTopicDLQ {
		p.log.Info("dlq message received", "payload", string(raw))
		return
	}

	var km model.KafkaMessage
	if err := json.Unmarshal(raw, &km); err != nil {
		p.log.Error("invalid kafka payload", "error", err)
		return
	}

	if err := p.svc.Process(ctx, km.NotificationID); err != nil {
		p.log.Warn("process notification", "id", km.NotificationID, "error", err)
	}
}
