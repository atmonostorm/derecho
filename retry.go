package derecho

import (
	"math"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

// RetryPolicy configures automatic retry behavior for activities.
type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
	MaxAttempts        int // 0 = unlimited
	NonRetryableErrors []journal.ErrorKind
}

// DefaultRetryPolicy returns a sensible default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        100 * time.Second,
		MaxAttempts:        0,
	}
}

// NoRetryPolicy returns a policy that allows no retries.
func NoRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// NextDelay calculates the backoff delay for the given attempt (1-indexed).
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := float64(p.InitialInterval) * math.Pow(p.BackoffCoefficient, float64(attempt-1))
	d := time.Duration(delay)
	if p.MaxInterval > 0 && d > p.MaxInterval {
		return p.MaxInterval
	}
	return d
}

// ShouldRetry returns true if the given error kind should be retried at the given attempt.
func (p RetryPolicy) ShouldRetry(kind journal.ErrorKind, attempt int) bool {
	if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
		return false
	}
	for _, nrk := range p.NonRetryableErrors {
		if nrk == kind {
			return false
		}
	}
	return true
}

// ToPayload converts the policy to a serializable payload.
func (p RetryPolicy) ToPayload() *journal.RetryPolicyPayload {
	return &journal.RetryPolicyPayload{
		InitialInterval:    p.InitialInterval,
		BackoffCoefficient: p.BackoffCoefficient,
		MaxInterval:        p.MaxInterval,
		MaxAttempts:        p.MaxAttempts,
		NonRetryableErrors: p.NonRetryableErrors,
	}
}

// PolicyFromPayload converts a serialized payload back to a RetryPolicy.
func PolicyFromPayload(p *journal.RetryPolicyPayload) RetryPolicy {
	if p == nil {
		return RetryPolicy{}
	}
	return RetryPolicy{
		InitialInterval:    p.InitialInterval,
		BackoffCoefficient: p.BackoffCoefficient,
		MaxInterval:        p.MaxInterval,
		MaxAttempts:        p.MaxAttempts,
		NonRetryableErrors: p.NonRetryableErrors,
	}
}

// NonRetryableError marks an error as permanent - no retries will be attempted.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// RetryableError provides hints for retry behavior.
type RetryableError struct {
	Err       error
	NextDelay time.Duration // 0 = use policy default
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps an error to indicate it should not be retried.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}

// RetryAfter wraps an error with a specific delay before the next retry.
func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err, NextDelay: delay}
}
