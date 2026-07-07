package model

import (
	"encoding/json"
	"time"

	"github.com/ezhigval/notification-hub/internal/config"
)

type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
	ChannelSMS   Channel = "sms"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusSent      Status = "sent"
	StatusRetrying  Status = "retrying"
	StatusDead      Status = "dead"
)

type Template struct {
	ID              int64     `json:"id"`
	Name            string    `json:"name"`
	Channel         Channel   `json:"channel"`
	SubjectTemplate string    `json:"subject_template,omitempty"`
	BodyTemplate    string    `json:"body_template"`
	CreatedAt       time.Time `json:"created_at"`
}

type Notification struct {
	ID             int64           `json:"id"`
	TemplateID     int64           `json:"template_id"`
	Channel        Channel         `json:"channel"`
	Recipient      string          `json:"recipient"`
	Variables      json.RawMessage `json:"variables"`
	Priority       config.Priority `json:"priority"`
	Status         Status          `json:"status"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Attempts       int             `json:"attempts"`
	NextRetryAt    *time.Time      `json:"next_retry_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type DeliveryAttempt struct {
	ID             int64     `json:"id"`
	NotificationID int64     `json:"notification_id"`
	Attempt        int       `json:"attempt"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type SendRequest struct {
	TemplateName   string          `json:"template_name"`
	Recipient      string          `json:"recipient"`
	Variables      json.RawMessage `json:"variables"`
	Priority       config.Priority `json:"priority"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type KafkaMessage struct {
	NotificationID int64           `json:"notification_id"`
	Priority       config.Priority `json:"priority"`
}
