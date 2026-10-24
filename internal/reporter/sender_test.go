package reporter

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/storage"
)

// clock is a settable time source for the sender.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func observedSender(t *testing.T, b *storage.Buffer, client interface {
	Send(context.Context, []byte) Result
}) (*Sender, *clock, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	s := NewSender(b, client, zap.New(core))
	// The buffered test reports are recent hours on the real clock, which the buffer's age cap runs on.
	c := &clock{now: time.Now().UTC()}
	s.now = c.Now
	s.jitter = func(d time.Duration) time.Duration { return d }
	s.spread = 0
	return s, c, logs
}

func TestSender_BackoffDoublesUpToTheCapAndResetsOnSuccess(t *testing.T) {
	b := newBuffer(t, 1)
	client := &scripted{results: []Result{{Outcome: Retry, Status: 503}}}
	s, c, _ := observedSender(t, b, client)

	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for i, w := range want {
		s.Drain(context.Background(), false)
		if client.calls != i+1 {
			t.Fatalf("attempt %d: calls = %d", i+1, client.calls)
		}
		if s.backoff != w {
			t.Fatalf("attempt %d: backoff = %s, want %s", i+1, s.backoff, w)
		}
		c.Advance(w - time.Nanosecond)
		s.Drain(context.Background(), false)
		if client.calls != i+1 {
			t.Fatalf("attempt %d: sent again before the %s backoff elapsed", i+1, w)
		}
		c.Advance(time.Nanosecond)
	}

	client.results = []Result{{Outcome: Accepted}}
	s.Drain(context.Background(), false)
	if length(t, b) != 0 || s.backoff != 0 || !s.retryAt.IsZero() {
		t.Fatalf("buffered = %d, backoff = %s, retryAt = %s; a success must reset the backoff", length(t, b), s.backoff, s.retryAt)
	}
}

func TestSender_RetryAfterCancellationDoesNotBackOff(t *testing.T) {
	b := newBuffer(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &cancelling{cancel: cancel}
	s, _, _ := observedSender(t, b, client)
	retriesBefore := testutil.ToFloat64(metrics.Reports.WithLabelValues("retry"))
	s.Drain(ctx, false)
	if s.backoff != 0 || !s.retryAt.IsZero() {
		t.Fatalf("backoff = %s; a send cut short by shutdown must not delay the next start", s.backoff)
	}
	if testutil.ToFloat64(metrics.Reports.WithLabelValues("retry"))-retriesBefore != 1 {
		t.Fatal("the retry was not counted")
	}
	if length(t, b) != 1 {
		t.Fatal("the report must be kept")
	}
}

type cancelling struct{ cancel context.CancelFunc }

func (c *cancelling) Send(context.Context, []byte) Result {
	c.cancel()
	return Result{Outcome: Retry, Message: "context canceled"}
}

func TestSender_DrainStopsOnACancelledContext(t *testing.T) {
	b := newBuffer(t, 1, 2)
	client := &scripted{results: []Result{{Outcome: Accepted}}}
	s, _, _ := observedSender(t, b, client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Drain(ctx, true)
	if client.calls != 0 || length(t, b) != 2 {
		t.Fatalf("calls = %d; a cancelled drain must not send", client.calls)
	}
}

func TestSender_UnauthorizedRemindsHourly(t *testing.T) {
	b := newBuffer(t, 1, 2)
	client := &scripted{results: []Result{{Outcome: Unauthorized, Status: 403}}}
	s, c, logs := observedSender(t, b, client)
	unauthorizedBefore := testutil.ToFloat64(metrics.Reports.WithLabelValues("unauthorized"))

	s.Drain(context.Background(), false)
	if testutil.ToFloat64(metrics.Reports.WithLabelValues("unauthorized"))-unauthorizedBefore != 1 {
		t.Fatal("the unauthorized attempt was not counted")
	}
	reminder := "reports are not being sent: the API refused the token; fix it and restart the agent"
	c.Advance(authReminderEach - time.Second)
	s.Drain(context.Background(), true)
	if logs.FilterMessage(reminder).Len() != 0 {
		t.Fatal("reminded before an hour passed")
	}
	c.Advance(time.Second)
	s.Drain(context.Background(), false)
	s.Drain(context.Background(), false)
	got := logs.FilterMessage(reminder).All()
	if len(got) != 1 || got[0].ContextMap()["buffered_reports"] != int64(2) {
		t.Fatalf("reminders = %+v, want one naming the two buffered reports", got)
	}
	if client.calls != 1 {
		t.Fatalf("calls = %d; an unauthorized token is never retried", client.calls)
	}
}

func TestSender_RejectedIsCountedAndDropped(t *testing.T) {
	b := newBuffer(t, 1)
	s, _, logs := observedSender(t, b, &scripted{results: []Result{{Outcome: Rejected, Status: 410, Message: "Gone"}}})
	rejectedBefore := testutil.ToFloat64(metrics.Reports.WithLabelValues("rejected"))
	droppedBefore := testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("rejected"))
	s.Drain(context.Background(), false)
	if testutil.ToFloat64(metrics.Reports.WithLabelValues("rejected"))-rejectedBefore != 1 ||
		testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("rejected"))-droppedBefore != 1 {
		t.Fatal("a rejected report must be counted as rejected and as dropped")
	}
	if length(t, b) != 0 {
		t.Fatal("a rejected report must be dropped")
	}
	entries := logs.FilterMessage("report refused and dropped: " + Advice(Result{Status: 410})).All()
	if len(entries) != 1 || entries[0].ContextMap()["status"] != int64(410) || entries[0].ContextMap()["message"] != "Gone" {
		t.Fatalf("log = %+v", entries)
	}
}

// vanishing removes the buffer directory while the report is in flight, so the sender's removal
// of the report cannot re-list the buffer.
type vanishing struct {
	dir     string
	outcome Outcome
	calls   int
}

func (v *vanishing) Send(context.Context, []byte) Result {
	v.calls++
	_ = os.RemoveAll(v.dir)
	return Result{Outcome: v.outcome, Status: 202}
}

func TestSender_StopsWhenRemovingASentReportFails(t *testing.T) {
	for _, tc := range []struct {
		outcome Outcome
		log     string
	}{
		{Accepted, "removing a sent report failed"},
		{Rejected, "removing a refused report failed"},
	} {
		dataDir := t.TempDir()
		b, err := storage.Open(dataDir, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(sampleReport(1)); err != nil {
			t.Fatal(err)
		}
		client := &vanishing{dir: filepath.Join(dataDir, "buffer"), outcome: tc.outcome}
		s, _, logs := observedSender(t, b, client)
		s.Drain(context.Background(), false)
		if client.calls != 1 || logs.FilterMessage(tc.log).Len() != 1 {
			t.Fatalf("outcome %v: calls = %d, logs = %v", tc.outcome, client.calls, logs.All())
		}
	}
}

func TestSender_StopsWhenTheBufferCannotBeRead(t *testing.T) {
	dataDir := t.TempDir()
	b, err := storage.Open(dataDir, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "buffer")); err != nil {
		t.Fatal(err)
	}
	client := &scripted{results: []Result{{Outcome: Accepted}}}
	s, _, logs := observedSender(t, b, client)
	s.Drain(context.Background(), true)
	if client.calls != 0 || logs.FilterMessage("reading the buffer failed").Len() != 1 {
		t.Fatalf("calls = %d, logs = %v", client.calls, logs.All())
	}
}

// signalling reports each send on a channel, so Run can be observed without sleeping.
type signalling struct{ sent chan struct{} }

func (s *signalling) Send(context.Context, []byte) Result {
	s.sent <- struct{}{}
	return Result{Outcome: Accepted, ID: "kr_1"}
}

func TestSender_RunDrainsOnWakeUntilCancelled(t *testing.T) {
	b := newBuffer(t, 1)
	client := &signalling{sent: make(chan struct{}, 1)}
	s := NewSender(b, client, zap.NewNop())

	s.Wake()
	s.Wake() // never blocks: a pending wake already covers it
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	select {
	case <-client.sent:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not drain on wake")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop when its context ended")
	}
	if length(t, b) != 0 {
		t.Fatal("the woken drain did not remove the accepted report")
	}
}

func TestSender_BackoffIsFullyJittered(t *testing.T) {
	b := newBuffer(t, 1)
	client := &scripted{results: []Result{{Outcome: Retry, Status: 503}}}
	s, c, _ := observedSender(t, b, client)
	var ceilings []time.Duration
	s.jitter = func(d time.Duration) time.Duration {
		ceilings = append(ceilings, d)
		return d / 3
	}

	s.Drain(context.Background(), false)
	if len(ceilings) != 1 || ceilings[0] != minBackoff {
		t.Fatalf("jitter drawn over %v, want one draw over the %s backoff", ceilings, minBackoff)
	}
	c.Advance(minBackoff/3 - time.Nanosecond)
	s.Drain(context.Background(), false)
	if client.calls != 1 {
		t.Fatal("sent again before the jittered delay elapsed")
	}
	c.Advance(time.Nanosecond)
	s.Drain(context.Background(), false)
	if client.calls != 2 || ceilings[1] != 2*minBackoff {
		t.Fatalf("calls = %d, ceilings = %v; want the retry at the jittered delay and the next ceiling doubled", client.calls, ceilings)
	}
}

func TestSender_HonoursRetryAfterUpToTheBackoffCap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retryAfter time.Duration
		wait       time.Duration
	}{
		{"longer than the backoff", 10 * time.Minute, 10 * time.Minute},
		{"shorter than the backoff", time.Second, minBackoff},
		{"beyond the cap", 3 * time.Hour, maxBackoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBuffer(t, 1)
			client := &scripted{results: []Result{{Outcome: Retry, Status: 429, RetryAfter: tc.retryAfter}, {Outcome: Accepted}}}
			s, c, _ := observedSender(t, b, client)
			s.Drain(context.Background(), false)
			c.Advance(tc.wait - time.Nanosecond)
			s.Drain(context.Background(), false)
			if client.calls != 1 {
				t.Fatalf("sent again before %s", tc.wait)
			}
			c.Advance(time.Nanosecond)
			s.Drain(context.Background(), false)
			if client.calls != 2 || length(t, b) != 0 {
				t.Fatalf("calls = %d; want the retry at %s", client.calls, tc.wait)
			}
		})
	}
}

// slow answers like scripted, each attempt taking took on the sender's clock.
type slow struct {
	scripted
	clock *clock
	took  time.Duration
}

func (s *slow) Send(ctx context.Context, body []byte) Result {
	s.clock.Advance(s.took)
	return s.scripted.Send(ctx, body)
}

// The wait before a retry runs from the failed attempt, however long the drain took to reach it.
func TestSender_WaitsFromTheFailureNotFromTheDrainsStart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hours   []int
		results []Result
		took    time.Duration
		wait    time.Duration
	}{
		{"a slow failed attempt", []int{1}, []Result{{Outcome: Retry, Status: 503}}, 2 * time.Minute, minBackoff},
		{"a failure after a long drain", []int{1, 2, 3}, []Result{{Outcome: Accepted}, {Outcome: Accepted}, {Outcome: Retry, Status: 503}}, 10 * time.Minute, minBackoff},
		{"a Retry-After after a long drain", []int{1, 2}, []Result{{Outcome: Accepted}, {Outcome: Retry, Status: 429, RetryAfter: 10 * time.Minute}}, 15 * time.Minute, 10 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBuffer(t, tc.hours...)
			client := &slow{scripted: scripted{results: tc.results}, took: tc.took}
			s, c, _ := observedSender(t, b, client)
			client.clock = c
			s.Drain(context.Background(), false)
			attempts := len(tc.results)
			if client.calls != attempts {
				t.Fatalf("calls = %d, want %d", client.calls, attempts)
			}
			c.Advance(tc.wait - time.Nanosecond)
			s.Drain(context.Background(), false)
			if client.calls != attempts {
				t.Fatalf("sent again %s after the failure, before the %s wait elapsed", tc.wait-time.Nanosecond, tc.wait)
			}
			c.Advance(time.Nanosecond)
			client.took = 0
			s.Drain(context.Background(), false)
			if client.calls != attempts+1 {
				t.Fatalf("calls = %d; want the retry %s after the failure", client.calls, tc.wait)
			}
		})
	}
}

func TestSender_HoldsAJustClosedWindowForItsSpread(t *testing.T) {
	b := newBuffer(t, 23)
	entry, _, err := b.Oldest()
	if err != nil {
		t.Fatal(err)
	}
	client := &scripted{results: []Result{{Outcome: Accepted}}}
	s, c, _ := observedSender(t, b, client)
	s.spread = 4 * time.Minute
	c.now = entry.WindowEnd.Add(time.Minute)

	s.Drain(context.Background(), false)
	if client.calls != 0 {
		t.Fatal("a window closed a minute ago was sent before this agent's spread")
	}
	c.Advance(3*time.Minute - time.Nanosecond)
	s.Drain(context.Background(), false)
	if client.calls != 0 {
		t.Fatal("sent before the spread elapsed")
	}
	c.Advance(time.Nanosecond)
	s.Drain(context.Background(), false)
	if client.calls != 1 {
		t.Fatal("not sent once the spread elapsed")
	}
}

func TestSender_SpreadDoesNotHoldOldWindowsOrTheShutdownFlush(t *testing.T) {
	b := newBuffer(t, 1, 23)
	client := &scripted{results: []Result{{Outcome: Accepted}}}
	s, c, _ := observedSender(t, b, client)
	s.spread = 4 * time.Minute
	entry, _, err := b.Oldest()
	if err != nil {
		t.Fatal(err)
	}
	c.now = entry.WindowStart.Add(23 * time.Hour)

	s.Drain(context.Background(), false)
	if client.calls != 1 || length(t, b) != 1 {
		t.Fatalf("calls = %d; a window closed long ago is sent at once, the just-closed one held", client.calls)
	}
	s.Drain(context.Background(), true)
	if client.calls != 2 || length(t, b) != 0 {
		t.Fatalf("calls = %d; the shutdown flush sends a held window", client.calls)
	}
}

func TestSender_SpreadHoldsOnlyWindowsEndingOnTheHour(t *testing.T) {
	dataDir := t.TempDir()
	b, err := storage.Open(dataDir, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	// The short first window after an install, and a window closed by a shutdown, end off the hour.
	start := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	firstWindow := sampleReport(23)
	firstWindow.WindowStart, firstWindow.WindowEnd = start.Add(10*time.Minute), start.Add(12*time.Minute)
	if err := b.Put(firstWindow); err != nil {
		t.Fatal(err)
	}
	client := &scripted{results: []Result{{Outcome: Accepted}}}
	s, c, _ := observedSender(t, b, client)
	s.spread = 4 * time.Minute
	c.now = firstWindow.WindowEnd.Add(time.Second)

	s.Drain(context.Background(), false)
	if client.calls != 1 || length(t, b) != 0 {
		t.Fatalf("calls = %d; a window that does not end on the hour is not held", client.calls)
	}
}

func TestNewSender_DrawsASpreadUnderFiveMinutes(t *testing.T) {
	for range 50 {
		s := NewSender(newBuffer(t), &scripted{results: []Result{{Outcome: Accepted}}}, zap.NewNop())
		if s.spread < 0 || s.spread >= maxSpread {
			t.Fatalf("spread = %s, want it in [0, %s)", s.spread, maxSpread)
		}
		if d := s.jitter(time.Minute); d < 0 || d >= time.Minute {
			t.Fatalf("jitter(1m) = %s, want it in [0, 1m)", d)
		}
	}
}

func TestRandomBelow_StaysInItsRange(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if got := randomBelow(d); got != 0 {
			t.Fatalf("randomBelow(%s) = %s, want 0", d, got)
		}
	}
	for range 100 {
		if got := randomBelow(time.Minute); got < 0 || got >= time.Minute {
			t.Fatalf("randomBelow(1m) = %s, outside [0, 1m)", got)
		}
	}
}
