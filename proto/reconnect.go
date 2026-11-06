package proto

import (
	"errors"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	"github.com/robert-malhotra/twine"
)

// Default ReconnectingClient backoff parameters (Twisted's
// ReconnectingClientFactory uses the same factor and maximum).
const (
	DefaultInitialDelay = time.Second
	DefaultMaxDelay     = 60 * time.Second
	DefaultFactor       = 1.6
)

// Option configures a ReconnectingClient.
type Option func(*ReconnectingClient)

// InitialDelay sets the backoff ceiling for the first retry. Default 1s.
func InitialDelay(d time.Duration) Option {
	return func(rc *ReconnectingClient) { rc.initial = d }
}

// MaxDelay caps the backoff ceiling. Default 60s.
func MaxDelay(d time.Duration) Option {
	return func(rc *ReconnectingClient) { rc.max = d }
}

// Factor sets the backoff growth factor. Default 1.6. Values below 1 are
// treated as 1.
func Factor(f float64) Option {
	return func(rc *ReconnectingClient) { rc.factor = f }
}

// MaxRetries retries at most n times after consecutive failures (connect
// errors or connection losses with no successful connect in between), i.e.
// it gives up after n+1 consecutive failed attempts. 0, the default, retries
// forever.
func MaxRetries(n int) Option {
	return func(rc *ReconnectingClient) { rc.maxRetries = n }
}

// OnStateChange registers fn to be called on the client's loop with true
// after each successful connect (before the protocol's ConnectionMade) and
// false after each connection loss (after the protocol's ConnectionLost).
func OnStateChange(fn func(connected bool)) Option {
	return func(rc *ReconnectingClient) { rc.onState = fn }
}

// DialOptions passes options to every ConnectTCP attempt.
func DialOptions(opts ...twine.DialOption) Option {
	return func(rc *ReconnectingClient) { rc.dialOpts = opts }
}

// ReconnectingClient is Twisted's ReconnectingClientFactory: it connects to
// addr on one loop and, whenever the connect fails or the connection is
// lost, retries with exponential backoff and full jitter. The n-th
// consecutive retry waits a uniformly random time in [0, min(MaxDelay,
// InitialDelay*Factor^(n-1))].
//
// The wrapped Factory builds the protocol for every successful connect; the
// client interposes a thin wrapper protocol to observe the connection's
// life. A successful connect resets the backoff automatically (Twisted's
// convention is for the protocol to call resetDelay once connected; Twine
// does it for you), so after an established connection is lost the first
// retry waits at most InitialDelay. ResetDelay is also exposed for callers
// that want to restart the backoff at other times.
//
// All state lives on the loop; every method is safe from any goroutine and
// is routed there with CallFromThread. Retrying stops on StopTrying, after
// MaxRetries consecutive failures, or when the engine stops.
type ReconnectingClient struct {
	loop    *twine.Loop
	addr    string
	factory twine.Factory

	initial, max time.Duration
	factor       float64
	maxRetries   int
	onState      func(bool)
	dialOpts     []twine.DialOption

	// Loop-affine state.
	started  bool
	stopped  bool
	inFlight bool
	failures int
	timer    *twine.Timer
	t        twine.Transport
	lastErr  error

	connected atomic.Bool
	attempts  atomic.Int64
	giveUp    atomic.Bool
}

// NewReconnectingClient returns a client that will connect to addr on l
// using f. Call Start to begin.
func NewReconnectingClient(l *twine.Loop, addr string, f twine.Factory, opts ...Option) *ReconnectingClient {
	rc := &ReconnectingClient{
		loop:    l,
		addr:    addr,
		factory: f,
		initial: DefaultInitialDelay,
		max:     DefaultMaxDelay,
		factor:  DefaultFactor,
	}
	for _, o := range opts {
		o(rc)
	}
	if rc.factor < 1 {
		rc.factor = 1
	}
	if rc.initial <= 0 {
		rc.initial = time.Millisecond
	}
	if rc.max < rc.initial {
		rc.max = rc.initial
	}
	return rc
}

// onLoop runs fn on the client's loop: inline when already there.
func (rc *ReconnectingClient) onLoop(fn func()) {
	if rc.loop.InLoop() {
		fn()
		return
	}
	rc.loop.CallFromThread(fn)
}

// Start begins connecting immediately. Calling Start after StopTrying
// resumes trying (with the backoff state reset); calling it while already
// trying is a no-op.
func (rc *ReconnectingClient) Start() {
	rc.onLoop(func() {
		if rc.started && !rc.stopped {
			return
		}
		rc.started, rc.stopped = true, false
		rc.giveUp.Store(false)
		rc.failures = 0
		if rc.inFlight || rc.t != nil || rc.timer != nil {
			return
		}
		rc.connect()
	})
}

// StopTrying stops reconnecting: a pending retry timer is cancelled and no
// further attempt is made. An established connection is left open (close
// it through its transport); when it is later lost it is not retried. An
// attempt already in flight is allowed to complete.
func (rc *ReconnectingClient) StopTrying() {
	rc.onLoop(func() {
		rc.stopped = true
		if rc.timer != nil {
			rc.timer.Stop()
			rc.timer = nil
		}
	})
}

// ResetDelay resets the backoff to InitialDelay and the consecutive-failure
// count to zero. It is called automatically after every successful connect.
func (rc *ReconnectingClient) ResetDelay() {
	rc.onLoop(func() { rc.failures = 0 })
}

// Connected reports whether a connection is currently established.
func (rc *ReconnectingClient) Connected() bool { return rc.connected.Load() }

// Attempts returns the total number of connect attempts made so far.
func (rc *ReconnectingClient) Attempts() int { return int(rc.attempts.Load()) }

// GaveUp reports whether the client stopped because MaxRetries was reached
// or the engine stopped.
func (rc *ReconnectingClient) GaveUp() bool { return rc.giveUp.Load() }

// Transport returns the current connection's transport, or nil. It is
// loop-affine: call it on the client's loop.
func (rc *ReconnectingClient) Transport() twine.Transport { return rc.t }

// LastError returns the most recent connect or connection-loss error. It is
// loop-affine: call it on the client's loop.
func (rc *ReconnectingClient) LastError() error { return rc.lastErr }

// attempt tracks one ConnectTCP call so that its Future rejection and its
// ConnectionLost (both possible when the protocol aborts inside
// ConnectionMade) schedule only one retry.
type attempt struct {
	rc   *ReconnectingClient
	made bool
}

func (rc *ReconnectingClient) connect() {
	rc.attempts.Add(1)
	rc.inFlight = true
	a := &attempt{rc: rc}
	fut := rc.loop.ConnectTCP(rc.addr, twine.FactoryFunc(a.build), rc.dialOpts...)
	fut.Catch(func(err error) {
		if a.made {
			return // ConnectionLost handles the retry
		}
		rc.inFlight = false
		rc.lastErr = err
		if errors.Is(err, twine.ErrEngineStopped) || errors.Is(err, twine.ErrLoopStopped) {
			rc.stopped = true
			rc.giveUp.Store(true)
			return
		}
		rc.retry()
	})
}

func (a *attempt) build(peer net.Addr) twine.Protocol {
	p := a.rc.factory.BuildProtocol(peer)
	if p == nil {
		p = nopProto{}
	}
	return &reconnectProto{a: a, inner: p}
}

// retry schedules the next attempt with full-jitter backoff.
func (rc *ReconnectingClient) retry() {
	if rc.stopped {
		return
	}
	rc.failures++
	if rc.maxRetries > 0 && rc.failures > rc.maxRetries {
		rc.stopped = true
		rc.giveUp.Store(true)
		return
	}
	rc.timer = rc.loop.CallLater(rc.jitter(rc.ceiling(rc.failures)), rc.fire)
}

// ceiling returns the backoff ceiling for the n-th consecutive retry:
// min(MaxDelay, InitialDelay*Factor^(n-1)).
func (rc *ReconnectingClient) ceiling(n int) time.Duration {
	c := float64(rc.initial)
	for i := 1; i < n && c < float64(rc.max); i++ {
		c *= rc.factor
	}
	return time.Duration(min(c, float64(rc.max)))
}

// jitter draws a full-jitter delay uniformly from [0, ceil].
func (rc *ReconnectingClient) jitter(ceil time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

func (rc *ReconnectingClient) fire() {
	rc.timer = nil // the handle is recycled once fired
	if rc.stopped {
		return
	}
	rc.connect()
}

func (rc *ReconnectingClient) setConnected(on bool) {
	rc.connected.Store(on)
	if rc.onState != nil {
		rc.onState(on)
	}
}

// reconnectProto forwards to the user's protocol and reports the
// connection's life to the client.
type reconnectProto struct {
	a     *attempt
	inner twine.Protocol
}

func (p *reconnectProto) ConnectionMade(t twine.Transport) {
	rc := p.a.rc
	p.a.made = true
	rc.inFlight = false
	rc.t = t
	rc.failures = 0 // ResetDelay
	rc.setConnected(true)
	p.inner.ConnectionMade(t)
}

func (p *reconnectProto) DataReceived(b []byte) { p.inner.DataReceived(b) }

func (p *reconnectProto) ConnectionLost(err error) {
	rc := p.a.rc
	p.inner.ConnectionLost(err)
	rc.t = nil
	rc.lastErr = err
	rc.setConnected(false)
	if errors.Is(err, twine.ErrShutdown) {
		rc.stopped = true
		rc.giveUp.Store(true)
		return
	}
	rc.retry()
}

type nopProto struct{}

func (nopProto) ConnectionMade(twine.Transport) {}
func (nopProto) DataReceived([]byte)            {}
func (nopProto) ConnectionLost(error)           {}
