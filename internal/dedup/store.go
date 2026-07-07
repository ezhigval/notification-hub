package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const prefix = "notify:dedup:"
const ttl = 72 * time.Hour

type Store struct {
	rdb *redis.Client
}

func NewStore(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func (s *Store) Seen(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, nil
	}
	n, err := s.rdb.Exists(ctx, prefix+key).Result()
	if err != nil {
		return false, fmt.Errorf("dedup exists: %w", err)
	}
	return n > 0, nil
}

func (s *Store) Mark(ctx context.Context, key string, notificationID int64) error {
	if key == "" {
		return nil
	}
	return s.rdb.Set(ctx, prefix+key, notificationID, ttl).Err()
}

func (s *Store) GetID(ctx context.Context, key string) (int64, bool, error) {
	if key == "" {
		return 0, false, nil
	}
	val, err := s.rdb.Get(ctx, prefix+key).Int64()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return val, true, nil
}
