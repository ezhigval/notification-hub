package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ezhigval/notification-hub/internal/config"
	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TemplateRepository struct {
	pool *pgxpool.Pool
}

func NewTemplateRepository(pool *pgxpool.Pool) *TemplateRepository {
	return &TemplateRepository{pool: pool}
}

func (r *TemplateRepository) Create(ctx context.Context, name string, ch model.Channel, subject, body string) (*model.Template, error) {
	var t model.Template
	err := r.pool.QueryRow(ctx, `
		INSERT INTO templates (name, channel, subject_template, body_template)
		VALUES ($1, $2, $3, $4)
		RETURNING id, name, channel, COALESCE(subject_template, ''), body_template, created_at
	`, name, ch, nullIfEmpty(subject), body).Scan(
		&t.ID, &t.Name, &t.Channel, &t.SubjectTemplate, &t.BodyTemplate, &t.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create template: %w", err)
	}
	return &t, nil
}

func (r *TemplateRepository) GetByID(ctx context.Context, id int64) (*model.Template, error) {
	var t model.Template
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, channel, COALESCE(subject_template, ''), body_template, created_at
		FROM templates WHERE id = $1
	`, id).Scan(&t.ID, &t.Name, &t.Channel, &t.SubjectTemplate, &t.BodyTemplate, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template by id: %w", err)
	}
	return &t, nil
}

func (r *TemplateRepository) GetByName(ctx context.Context, name string) (*model.Template, error) {
	var t model.Template
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, channel, COALESCE(subject_template, ''), body_template, created_at
		FROM templates WHERE name = $1
	`, name).Scan(&t.ID, &t.Name, &t.Channel, &t.SubjectTemplate, &t.BodyTemplate, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}
	return &t, nil
}

func (r *TemplateRepository) List(ctx context.Context) ([]model.Template, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, channel, COALESCE(subject_template, ''), body_template, created_at
		FROM templates ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.Template
	for rows.Next() {
		var t model.Template
		if err := rows.Scan(&t.ID, &t.Name, &t.Channel, &t.SubjectTemplate, &t.BodyTemplate, &t.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, t)
	}
	return list, rows.Err()
}

type NotificationRepository struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{pool: pool}
}

func (r *NotificationRepository) Create(ctx context.Context, n *model.Notification) (*model.Notification, error) {
	var out model.Notification
	err := r.pool.QueryRow(ctx, `
		INSERT INTO notifications (template_id, channel, recipient, variables, priority, status, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, template_id, channel, recipient, variables, priority, status,
			COALESCE(idempotency_key, ''), attempts, next_retry_at, COALESCE(last_error, ''),
			created_at, updated_at
	`, n.TemplateID, n.Channel, n.Recipient, n.Variables, n.Priority, n.Status, nullIfEmpty(n.IdempotencyKey)).Scan(
		&out.ID, &out.TemplateID, &out.Channel, &out.Recipient, &out.Variables, &out.Priority, &out.Status,
		&out.IdempotencyKey, &out.Attempts, &out.NextRetryAt, &out.LastError, &out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create notification: %w", err)
	}
	return &out, nil
}

func (r *NotificationRepository) GetByID(ctx context.Context, id int64) (*model.Notification, error) {
	var n model.Notification
	err := r.pool.QueryRow(ctx, `
		SELECT id, template_id, channel, recipient, variables, priority, status,
			COALESCE(idempotency_key, ''), attempts, next_retry_at, COALESCE(last_error, ''),
			created_at, updated_at
		FROM notifications WHERE id = $1
	`, id).Scan(
		&n.ID, &n.TemplateID, &n.Channel, &n.Recipient, &n.Variables, &n.Priority, &n.Status,
		&n.IdempotencyKey, &n.Attempts, &n.NextRetryAt, &n.LastError, &n.CreatedAt, &n.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (r *NotificationRepository) GetByIdempotencyKey(ctx context.Context, key string) (*model.Notification, error) {
	var n model.Notification
	err := r.pool.QueryRow(ctx, `
		SELECT id, template_id, channel, recipient, variables, priority, status,
			COALESCE(idempotency_key, ''), attempts, next_retry_at, COALESCE(last_error, ''),
			created_at, updated_at
		FROM notifications WHERE idempotency_key = $1
	`, key).Scan(
		&n.ID, &n.TemplateID, &n.Channel, &n.Recipient, &n.Variables, &n.Priority, &n.Status,
		&n.IdempotencyKey, &n.Attempts, &n.NextRetryAt, &n.LastError, &n.CreatedAt, &n.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (r *NotificationRepository) MarkSent(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications SET status = $1, updated_at = NOW(), last_error = NULL WHERE id = $2
	`, model.StatusSent, id)
	return err
}

func (r *NotificationRepository) MarkRetrying(ctx context.Context, id int64, attempts int, nextRetry time.Time, lastErr string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications SET status = $1, attempts = $2, next_retry_at = $3, last_error = $4, updated_at = NOW()
		WHERE id = $5
	`, model.StatusRetrying, attempts, nextRetry, lastErr, id)
	return err
}

func (r *NotificationRepository) MarkDead(ctx context.Context, id int64, lastErr string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications SET status = $1, last_error = $2, updated_at = NOW() WHERE id = $3
	`, model.StatusDead, lastErr, id)
	return err
}

func (r *NotificationRepository) ListDueRetries(ctx context.Context, limit int) ([]model.Notification, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, template_id, channel, recipient, variables, priority, status,
			COALESCE(idempotency_key, ''), attempts, next_retry_at, COALESCE(last_error, ''),
			created_at, updated_at
		FROM notifications
		WHERE status = $1 AND next_retry_at <= NOW()
		ORDER BY next_retry_at
		LIMIT $2
	`, model.StatusRetrying, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanNotifications(rows)
}

func (r *NotificationRepository) LogAttempt(ctx context.Context, notificationID int64, attempt int, status, errMsg string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO delivery_attempts (notification_id, attempt, status, error)
		VALUES ($1, $2, $3, $4)
	`, notificationID, attempt, status, nullIfEmpty(errMsg))
	return err
}

func (r *NotificationRepository) ListAttempts(ctx context.Context, notificationID int64) ([]model.DeliveryAttempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, notification_id, attempt, status, COALESCE(error, ''), created_at
		FROM delivery_attempts WHERE notification_id = $1 ORDER BY attempt
	`, notificationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.DeliveryAttempt
	for rows.Next() {
		var a model.DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.Attempt, &a.Status, &a.Error, &a.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	return list, rows.Err()
}

func scanNotifications(rows pgx.Rows) ([]model.Notification, error) {
	var list []model.Notification
	for rows.Next() {
		var n model.Notification
		if err := rows.Scan(
			&n.ID, &n.TemplateID, &n.Channel, &n.Recipient, &n.Variables, &n.Priority, &n.Status,
			&n.IdempotencyKey, &n.Attempts, &n.NextRetryAt, &n.LastError, &n.CreatedAt, &n.UpdatedAt,
		); err != nil {
			return nil, err
		}
		list = append(list, n)
	}
	return list, rows.Err()
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func DefaultPriority(p config.Priority) config.Priority {
	if p == "" {
		return config.PriorityNormal
	}
	return p
}
