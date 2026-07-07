package channel

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type Deliverer interface {
	Send(ctx context.Context, recipient, subject, body string) error
}

type EmailMock struct {
	log *slog.Logger
}

func NewEmailMock(log *slog.Logger) *EmailMock {
	return &EmailMock{log: log}
}

func (e *EmailMock) Send(ctx context.Context, recipient, subject, body string) error {
	e.log.Info("email sent (mock)", "to", recipient, "subject", subject, "body_len", len(body))
	if strings.Contains(recipient, "fail-email") {
		return fmt.Errorf("smtp rejected: mailbox unavailable")
	}
	return nil
}

type PushMock struct {
	log *slog.Logger
}

func NewPushMock(log *slog.Logger) *PushMock {
	return &PushMock{log: log}
}

func (p *PushMock) Send(ctx context.Context, recipient, _, body string) error {
	p.log.Info("push sent (mock)", "device", recipient, "body", body)
	if strings.Contains(recipient, "fail-push") {
		return fmt.Errorf("push gateway timeout")
	}
	return nil
}

type SMSMock struct {
	log *slog.Logger
}

func NewSMSMock(log *slog.Logger) *SMSMock {
	return &SMSMock{log: log}
}

func (s *SMSMock) Send(ctx context.Context, recipient, _, body string) error {
	s.log.Info("sms sent (mock)", "phone", recipient, "body", body)
	if strings.Contains(recipient, "fail-sms") {
		return fmt.Errorf("carrier rejected")
	}
	return nil
}

type Registry struct {
	email *EmailMock
	push  *PushMock
	sms   *SMSMock
}

func NewRegistry(log *slog.Logger) *Registry {
	return &Registry{
		email: NewEmailMock(log),
		push:  NewPushMock(log),
		sms:   NewSMSMock(log),
	}
}

func (r *Registry) Get(channel string) (Deliverer, error) {
	switch channel {
	case "email":
		return r.email, nil
	case "push":
		return r.push, nil
	case "sms":
		return r.sms, nil
	default:
		return nil, fmt.Errorf("unknown channel: %s", channel)
	}
}
