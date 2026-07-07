package retry

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	if Backoff(1) != 5*time.Second {
		t.Fatalf("attempt 1")
	}
	if Backoff(2) != 10*time.Second {
		t.Fatalf("attempt 2")
	}
	if Backoff(10) != 300*time.Second {
		t.Fatalf("cap at 300s")
	}
}
