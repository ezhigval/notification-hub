-- +goose Up
CREATE TABLE IF NOT EXISTS templates (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    channel          TEXT NOT NULL,
    subject_template TEXT,
    body_template    TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
    id              BIGSERIAL PRIMARY KEY,
    template_id     BIGINT NOT NULL REFERENCES templates(id),
    channel         TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    variables       JSONB NOT NULL DEFAULT '{}',
    priority        TEXT NOT NULL DEFAULT 'normal',
    status          TEXT NOT NULL DEFAULT 'pending',
    idempotency_key TEXT UNIQUE,
    attempts        INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id              BIGSERIAL PRIMARY KEY,
    notification_id BIGINT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    attempt         INT NOT NULL,
    status          TEXT NOT NULL,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_retry ON notifications(next_retry_at) WHERE status = 'retrying';
CREATE INDEX idx_notifications_status ON notifications(status);

-- +goose Down
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS templates;
