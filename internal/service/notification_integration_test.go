package service_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/ezhigval/notification-hub/internal/channel"
	"github.com/ezhigval/notification-hub/internal/config"
	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/ezhigval/notification-hub/internal/repository"
	"github.com/ezhigval/notification-hub/internal/service"
	"github.com/ezhigval/notification-hub/internal/template"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestProcess_deliversEmail(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pg, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notify"),
		postgres.WithUsername("notify"),
		postgres.WithPassword("notify"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	connStr, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	applySchema(t, ctx, pool)

	cfg := config.Config{MaxDeliveryAttempts: 3}
	templates := repository.NewTemplateRepository(pool)
	notifs := repository.NewNotificationRepository(pool)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	svc := service.NewNotificationService(
		cfg, templates, notifs, nil, nil,
		template.NewEngine(), channel.NewRegistry(log), log,
	)

	tmpl, err := svc.CreateTemplate(ctx, "welcome", model.ChannelEmail, "Hi {{.Name}}", "Welcome {{.Name}}!")
	require.NoError(t, err)

	n, err := notifs.Create(ctx, &model.Notification{
		TemplateID: tmpl.ID,
		Channel:    model.ChannelEmail,
		Recipient:  "user@example.com",
		Variables:  []byte(`{"Name":"Valentin"}`),
		Priority:   config.PriorityNormal,
		Status:     model.StatusPending,
	})
	require.NoError(t, err)

	require.NoError(t, svc.Process(ctx, n.ID))

	updated, err := notifs.GetByID(ctx, n.ID)
	require.NoError(t, err)
	require.Equal(t, model.StatusSent, updated.Status)
}

func applySchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE templates (
			id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, channel TEXT NOT NULL,
			subject_template TEXT, body_template TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE notifications (
			id BIGSERIAL PRIMARY KEY, template_id BIGINT NOT NULL REFERENCES templates(id),
			channel TEXT NOT NULL, recipient TEXT NOT NULL, variables JSONB NOT NULL DEFAULT '{}',
			priority TEXT NOT NULL DEFAULT 'normal', status TEXT NOT NULL DEFAULT 'pending',
			idempotency_key TEXT UNIQUE, attempts INT NOT NULL DEFAULT 0,
			next_retry_at TIMESTAMPTZ, last_error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE delivery_attempts (
			id BIGSERIAL PRIMARY KEY, notification_id BIGINT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
			attempt INT NOT NULL, status TEXT NOT NULL, error TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	require.NoError(t, err)
}
