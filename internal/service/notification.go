package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ezhigval/notification-hub/internal/channel"
	"github.com/ezhigval/notification-hub/internal/config"
	"github.com/ezhigval/notification-hub/internal/dedup"
	"github.com/ezhigval/notification-hub/internal/kafka"
	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/ezhigval/notification-hub/internal/repository"
	"github.com/ezhigval/notification-hub/internal/retry"
	"github.com/ezhigval/notification-hub/internal/template"
	"log/slog"
)

var (
	ErrTemplateNotFound = errors.New("template not found")
	ErrNotFound         = errors.New("notification not found")
	ErrInvalidRequest   = errors.New("invalid request")
)

type NotificationService struct {
	cfg       config.Config
	templates *repository.TemplateRepository
	notifs    *repository.NotificationRepository
	dedup     *dedup.Store
	publisher *kafka.Publisher
	engine    *template.Engine
	channels  *channel.Registry
	log       *slog.Logger
}

func NewNotificationService(
	cfg config.Config,
	templates *repository.TemplateRepository,
	notifs *repository.NotificationRepository,
	dedup *dedup.Store,
	publisher *kafka.Publisher,
	engine *template.Engine,
	channels *channel.Registry,
	log *slog.Logger,
) *NotificationService {
	return &NotificationService{
		cfg:       cfg,
		templates: templates,
		notifs:    notifs,
		dedup:     dedup,
		publisher: publisher,
		engine:    engine,
		channels:  channels,
		log:       log,
	}
}

func (s *NotificationService) CreateTemplate(ctx context.Context, name string, ch model.Channel, subject, body string) (*model.Template, error) {
	name = strings.TrimSpace(name)
	if name == "" || body == "" {
		return nil, ErrInvalidRequest
	}
	return s.templates.Create(ctx, name, ch, subject, body)
}

func (s *NotificationService) ListTemplates(ctx context.Context) ([]model.Template, error) {
	list, err := s.templates.List(ctx)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return []model.Template{}, nil
	}
	return list, nil
}

func (s *NotificationService) Send(ctx context.Context, req model.SendRequest) (*model.Notification, error) {
	req.Recipient = strings.TrimSpace(req.Recipient)
	if req.TemplateName == "" || req.Recipient == "" {
		return nil, ErrInvalidRequest
	}
	req.Priority = repository.DefaultPriority(req.Priority)

	if req.IdempotencyKey != "" {
		if id, ok, err := s.dedup.GetID(ctx, req.IdempotencyKey); err != nil {
			return nil, err
		} else if ok {
			return s.notifs.GetByID(ctx, id)
		}
		existing, err := s.notifs.GetByIdempotencyKey(ctx, req.IdempotencyKey)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			_ = s.dedup.Mark(ctx, req.IdempotencyKey, existing.ID)
			return existing, nil
		}
	}

	tmpl, err := s.templates.GetByName(ctx, req.TemplateName)
	if err != nil {
		return nil, err
	}
	if tmpl == nil {
		return nil, ErrTemplateNotFound
	}

	vars := req.Variables
	if vars == nil {
		vars = json.RawMessage(`{}`)
	}

	n := &model.Notification{
		TemplateID:     tmpl.ID,
		Channel:        tmpl.Channel,
		Recipient:      req.Recipient,
		Variables:      vars,
		Priority:       req.Priority,
		Status:         model.StatusPending,
		IdempotencyKey: req.IdempotencyKey,
	}
	created, err := s.notifs.Create(ctx, n)
	if err != nil {
		return nil, err
	}

	if req.IdempotencyKey != "" {
		_ = s.dedup.Mark(ctx, req.IdempotencyKey, created.ID)
	}

	if err := s.enqueue(ctx, created); err != nil {
		return nil, err
	}
	return created, nil
}

func (s *NotificationService) enqueue(ctx context.Context, n *model.Notification) error {
	topic := s.cfg.TopicForPriority(n.Priority)
	return s.publisher.Publish(ctx, topic, model.KafkaMessage{
		NotificationID: n.ID,
		Priority:       n.Priority,
	})
}

func (s *NotificationService) GetNotification(ctx context.Context, id int64) (*model.Notification, error) {
	n, err := s.notifs.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrNotFound
	}
	return n, nil
}

func (s *NotificationService) ListAttempts(ctx context.Context, id int64) ([]model.DeliveryAttempt, error) {
	if _, err := s.GetNotification(ctx, id); err != nil {
		return nil, err
	}
	list, err := s.notifs.ListAttempts(ctx, id)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return []model.DeliveryAttempt{}, nil
	}
	return list, nil
}

func (s *NotificationService) Process(ctx context.Context, notificationID int64) error {
	n, err := s.notifs.GetByID(ctx, notificationID)
	if err != nil {
		return err
	}
	if n == nil {
		return ErrNotFound
	}
	if n.Status == model.StatusSent || n.Status == model.StatusDead {
		return nil
	}

	tmplByID, err := s.templates.GetByID(ctx, n.TemplateID)
	if err != nil {
		return err
	}
	if tmplByID == nil {
		return ErrTemplateNotFound
	}

	var vars map[string]any
	if err := json.Unmarshal(n.Variables, &vars); err != nil {
		return fmt.Errorf("parse variables: %w", err)
	}

	body, err := s.engine.Render(tmplByID.BodyTemplate, vars)
	if err != nil {
		return s.failDelivery(ctx, n, err)
	}
	subject := ""
	if tmplByID.SubjectTemplate != "" {
		subject, err = s.engine.Render(tmplByID.SubjectTemplate, vars)
		if err != nil {
			return s.failDelivery(ctx, n, err)
		}
	}

	deliverer, err := s.channels.Get(string(n.Channel))
	if err != nil {
		return s.failDelivery(ctx, n, err)
	}

	attempt := n.Attempts + 1
	if err := deliverer.Send(ctx, n.Recipient, subject, body); err != nil {
		_ = s.notifs.LogAttempt(ctx, n.ID, attempt, "failed", err.Error())
		return s.failDelivery(ctx, n, err)
	}

	_ = s.notifs.LogAttempt(ctx, n.ID, attempt, "sent", "")
	return s.notifs.MarkSent(ctx, n.ID)
}

func (s *NotificationService) failDelivery(ctx context.Context, n *model.Notification, deliveryErr error) error {
	attempt := n.Attempts + 1
	if attempt >= s.cfg.MaxDeliveryAttempts {
		_ = s.notifs.MarkDead(ctx, n.ID, deliveryErr.Error())
		_ = s.publisher.Publish(ctx, s.cfg.KafkaTopicDLQ, model.KafkaMessage{
			NotificationID: n.ID,
			Priority:       n.Priority,
		})
		s.log.Warn("notification moved to DLQ", "id", n.ID, "error", deliveryErr)
		return deliveryErr
	}

	next := time.Now().UTC().Add(retry.Backoff(attempt))
	if err := s.notifs.MarkRetrying(ctx, n.ID, attempt, next, deliveryErr.Error()); err != nil {
		return err
	}
	s.log.Info("notification scheduled for retry", "id", n.ID, "attempt", attempt, "next", next)
	return deliveryErr
}

func (s *NotificationService) RepublishDueRetries(ctx context.Context) error {
	due, err := s.notifs.ListDueRetries(ctx, 50)
	if err != nil {
		return err
	}
	for _, n := range due {
		if err := s.enqueue(ctx, &n); err != nil {
			s.log.Error("retry enqueue failed", "id", n.ID, "error", err)
		}
	}
	return nil
}
