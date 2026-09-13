package discordgo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestSession(t *testing.T, handler http.HandlerFunc) (*Session, *httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	s, err := New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	return s, srv, &hits
}

// A 429 that says nothing about when to retry must fail the call once,
// not retry in a zero-delay loop until the context runs out.
func TestRateLimit429WithoutRetryInfo(t *testing.T) {
	s, srv, hits := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":0,"message":"You are being blocked"}`))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := s.RequestWithBucketID("GET", srv.URL+"/a", nil, "a", WithContext(ctx))

	if _, ok := err.(*RateLimitError); !ok {
		t.Fatalf("want *RateLimitError, got %T: %v", err, err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Errorf("want 1 request, got %d", n)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("call took %v, should not have waited", time.Since(start))
	}
}

// An IP block carries Retry-After in the header only. A wait longer than the
// request deadline fails immediately, and every other bucket is parked too.
func TestRateLimitBlockParksAllBuckets(t *testing.T) {
	s, srv, hits := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3080")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`<html>blocked</html>`))
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := s.RequestWithBucketID("GET", srv.URL+"/a", nil, "a", WithContext(ctx))
	if _, ok := err.(*RateLimitError); !ok {
		t.Fatalf("want *RateLimitError, got %T: %v", err, err)
	}

	// Different bucket, never reaches the server.
	_, err = s.RequestWithBucketID("GET", srv.URL+"/b", nil, "b", WithContext(ctx))
	rlErr, ok := err.(*RateLimitError)
	if !ok {
		t.Fatalf("want *RateLimitError on parked bucket, got %T: %v", err, err)
	}
	if !rlErr.Global || rlErr.RetryAfter < 3000*time.Second {
		t.Errorf("parked bucket error should carry the block: %+v", rlErr.TooManyRequests)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Errorf("want 1 request total, got %d", n)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("calls took %v, should not have waited", time.Since(start))
	}
}

// A bucket 429 with retry_after is retried after that long, bounded by
// MaxRestRetries.
func TestRateLimit429RetryBounded(t *testing.T) {
	s, srv, hits := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Bucket", "abc")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"You are being rate limited.","retry_after":0.05,"global":false}`))
	})
	s.MaxRestRetries = 2

	start := time.Now()
	_, err := s.RequestWithBucketID("GET", srv.URL+"/a", nil, "a")
	if _, ok := err.(*RateLimitError); !ok {
		t.Fatalf("want *RateLimitError, got %T: %v", err, err)
	}
	if n := atomic.LoadInt32(hits); n != 3 {
		t.Errorf("want initial + 2 retries = 3 requests, got %d", n)
	}
	if el := time.Since(start); el < 100*time.Millisecond || el > time.Second {
		t.Errorf("expected ~100ms of retry sleeps, took %v", el)
	}
}

func TestRateLimit429ThenSuccess(t *testing.T) {
	var n int32
	s, srv, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"You are being rate limited.","retry_after":0.05}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if _, err := s.RequestWithBucketID("GET", srv.URL+"/a", nil, "a"); err != nil {
		t.Fatalf("expected success after one retry, got %v", err)
	}
}

// SetGlobalRate spaces requests across buckets.
func TestRateLimitGlobalPace(t *testing.T) {
	s, srv, hits := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	s.Ratelimiter.SetGlobalRate(20)

	start := time.Now()
	for i := 0; i < 10; i++ {
		if _, err := s.RequestWithBucketID("GET", srv.URL+"/x", nil, "bucket"+string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 450*time.Millisecond {
		t.Errorf("10 requests at 20/s should take >=450ms, took %v", el)
	}
	if n := atomic.LoadInt32(hits); n != 10 {
		t.Errorf("want 10 requests, got %d", n)
	}
}
