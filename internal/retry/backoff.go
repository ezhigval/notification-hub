package retry

import "time"

func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	secs := 5 << (attempt - 1) // 5s, 10s, 20s...
	if secs > 300 {
		secs = 300
	}
	return time.Duration(secs) * time.Second
}
