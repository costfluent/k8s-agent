package reporter

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/storage"
)

const (
	minBackoff       = 30 * time.Second
	maxBackoff       = 15 * time.Minute
	authReminderEach = time.Hour
)

// Sender drains the buffer oldest first, off the poll loop.
type Sender struct {
	buffer *storage.Buffer
	client interface {
		Send(ctx context.Context, gzipBody []byte) Result
	}
	logger *zap.Logger
	now    func() time.Time
	wake   chan struct{}

	mu           sync.Mutex
	backoff      time.Duration
	retryAt      time.Time
	unauthorized bool
	remindedAt   time.Time
}

// NewSender drains buffer through client.
func NewSender(buffer *storage.Buffer, client interface {
	Send(ctx context.Context, gzipBody []byte) Result
}, logger *zap.Logger) *Sender {
	return &Sender{buffer: buffer, client: client, logger: logger.Named("sender"), now: time.Now, wake: make(chan struct{}, 1)}
}

// Wake asks the sender to drain now. It never blocks.
func (s *Sender) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run drains on every wake until ctx ends.
func (s *Sender) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			s.Drain(ctx, false)
		}
	}
}

// Drain sends buffered reports until the buffer is empty or an attempt fails. force ignores the
// backoff, for the shutdown flush; an unauthorized token is never retried.
func (s *Sender) Drain(ctx context.Context, force bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.unauthorized {
		if now.Sub(s.remindedAt) >= authReminderEach {
			s.remindedAt = now
			n, _ := s.buffer.Len()
			s.logger.Error("reports are not being sent: the API refused the token; fix it and restart the agent", zap.Int("buffered_reports", n))
		}
		return
	}
	if !force && now.Before(s.retryAt) {
		return
	}
	for ctx.Err() == nil {
		entry, ok, err := s.buffer.Oldest()
		if err != nil {
			s.logger.Error("reading the buffer failed", zap.Error(err))
			return
		}
		if !ok {
			return
		}
		metrics.ReportBytes.Observe(float64(len(entry.Body)))
		res := s.client.Send(ctx, entry.Body)
		fields := []zap.Field{zap.String("report", entry.Name), zap.Int("status", res.Status), zap.String("message", res.Message)}

		switch res.Outcome {
		case Accepted:
			metrics.Reports.WithLabelValues("accepted").Inc()
			s.backoff, s.retryAt = 0, time.Time{}
			s.logger.Info("report accepted", zap.String("report", entry.Name), zap.String("id", res.ID))
			if err := s.buffer.Remove(entry.Name); err != nil {
				s.logger.Error("removing a sent report failed", zap.Error(err))
				return
			}
		case Rejected:
			metrics.Reports.WithLabelValues("rejected").Inc()
			metrics.BufferDropped.WithLabelValues("rejected").Inc()
			s.logger.Error("report refused and dropped: "+Advice(res.Status), fields...)
			if err := s.buffer.Remove(entry.Name); err != nil {
				s.logger.Error("removing a refused report failed", zap.Error(err))
				return
			}
		case Unauthorized:
			metrics.Reports.WithLabelValues("unauthorized").Inc()
			s.unauthorized, s.remindedAt = true, now
			s.logger.Error("report refused: "+Advice(res.Status), fields...)
			return
		default:
			metrics.Reports.WithLabelValues("retry").Inc()
			if ctx.Err() != nil {
				return
			}
			s.backoff = min(max(2*s.backoff, minBackoff), maxBackoff)
			s.retryAt = now.Add(s.backoff)
			s.logger.Warn("report not sent: "+Advice(res.Status), append(fields, zap.Duration("retry_in", s.backoff))...)
			return
		}
	}
}
