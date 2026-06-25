package main

import (
	"context"
	"time"
)

// RateLimiter enforces a global, steady request rate that is shared across all
// goroutines holding a pointer to it. It is backed by a single time.Ticker, so
// no matter how many workers call Wait, only one of them proceeds per tick.
// This lets us decouple I/O parallelism (worker count) from the rate at which we
// actually hit the network, which is the thing YouTube throttles on.
type RateLimiter struct {
	ticker *time.Ticker
}

// NewRateLimiter builds a limiter that allows roughly perSecond requests per
// second in aggregate across every caller of Wait.
func NewRateLimiter(perSecond float64) *RateLimiter {
	interval := time.Duration(float64(time.Second) / perSecond)
	return &RateLimiter{ticker: time.NewTicker(interval)}
}

// Wait blocks until the next token is available or the context is cancelled.
// It returns ctx.Err() on cancellation so callers can abort cleanly.
func (r *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-r.ticker.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop releases the underlying ticker. Safe to call once the limiter is no
// longer needed (e.g. after all workers have finished).
func (r *RateLimiter) Stop() {
	r.ticker.Stop()
}
