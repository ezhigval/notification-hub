package config

import (
	"time"

	"github.com/ezhigval/go-toolkit/config"
)

type Config struct {
	Port      string `env:"PORT" envDefault:"8088"`
	LogLevel  string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat string `env:"LOG_FORMAT" envDefault:"json"`

	DatabaseURL string `env:"DATABASE_URL,required"`

	RedisAddr     string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	RedisPassword string `env:"REDIS_PASSWORD"`
	RedisDB       int    `env:"REDIS_DB" envDefault:"0"`

	KafkaBrokers        []string      `env:"KAFKA_BROKERS" envDefault:"localhost:9093"`
	KafkaTopicHigh      string        `env:"KAFKA_TOPIC_HIGH" envDefault:"notifications.high"`
	KafkaTopicNormal    string        `env:"KAFKA_TOPIC_NORMAL" envDefault:"notifications.normal"`
	KafkaTopicLow       string        `env:"KAFKA_TOPIC_LOW" envDefault:"notifications.low"`
	KafkaTopicDLQ       string        `env:"KAFKA_TOPIC_DLQ" envDefault:"notifications.dlq"`
	KafkaConsumerGroup  string        `env:"KAFKA_CONSUMER_GROUP" envDefault:"notification-hub"`
	MaxDeliveryAttempts int           `env:"MAX_DELIVERY_ATTEMPTS" envDefault:"3"`
	RetryPollInterval   time.Duration `env:"RETRY_POLL_INTERVAL" envDefault:"5s"`
	WorkerPoolSize      int           `env:"WORKER_POOL_SIZE" envDefault:"4"`
	EnableWorker        bool          `env:"ENABLE_WORKER" envDefault:"true"`
}

func MustLoad() Config {
	return config.MustLoad[Config]()
}

func (c Config) TopicForPriority(p Priority) string {
	switch p {
	case PriorityHigh:
		return c.KafkaTopicHigh
	case PriorityLow:
		return c.KafkaTopicLow
	default:
		return c.KafkaTopicNormal
	}
}

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)
