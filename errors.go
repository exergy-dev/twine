package twine

import (
	"errors"
	"fmt"
	"runtime"
)

// Sentinel errors. All are comparable with errors.Is.
var (
	// ErrClosed rejects a ConnectTCP or Dial Future when the connection was
	// closed during ConnectionMade (for example by Abort). Writes on a closed
	// transport do not return it: they are silent no-ops counted in
	// MetricsSnapshot.WriteAfterClose.
	ErrClosed = errors.New("twine: transport closed")

	// ErrWriteBufferFull aborts a connection whose pending writes exceed
	// WriteBufferLimit.
	ErrWriteBufferFull = errors.New("twine: write buffer full")

	// ErrAwaitOnLoop is the panic value when Future.Await is called from a
	// loop goroutine. Awaiting on the loop would deadlock it.
	ErrAwaitOnLoop = errors.New("twine: Future.Await called on its own loop")

	// ErrIdleTimeout is passed to ConnectionLost when the idle timer fires.
	ErrIdleTimeout = errors.New("twine: idle timeout")

	// ErrConnectTimeout rejects a ConnectTCP Future when the dial deadline
	// passes before the connection completes.
	ErrConnectTimeout = errors.New("twine: connect timeout")

	// ErrEngineStopped rejects ConnectTCP, Dial, DeferToThread, ListenTCP
	// and ListenUDP after Engine.Stop.
	ErrEngineStopped = errors.New("twine: engine stopped")

	// ErrNilProtocol rejects a ConnectTCP or Dial Future whose Factory
	// returned a nil Protocol; the connection is closed.
	ErrNilProtocol = errors.New("twine: factory returned a nil Protocol")

	// ErrNoFutures rejects the Future returned by Any when it is given no
	// Futures to wait for.
	ErrNoFutures = errors.New("twine: Any called with no Futures")

	// ErrRunTwice is returned by a second call to Engine.Run while the first
	// is running or after it has returned. An Engine runs once.
	ErrRunTwice = errors.New("twine: Engine.Run called twice")

	// ErrConnectionReset is passed to ConnectionLost when the peer resets the
	// connection (ECONNRESET / EPIPE).
	ErrConnectionReset = errors.New("twine: connection reset by peer")

	// ErrNotInLoop is the panic value when a loop-affine method is called off
	// its loop in a debug build.
	ErrNotInLoop = errors.New("twine: method called off its owning loop")

	// ErrShutdown is passed to ConnectionLost when the engine shuts down a
	// connection that still had pending writes after ShutdownTimeout.
	ErrShutdown = errors.New("twine: engine shutdown")
)

// PanicError wraps a panic recovered from protocol or Future callback code.
type PanicError struct {
	// Value is the recovered panic value.
	Value any
	// Stack is the goroutine stack at the point of recovery.
	Stack []byte
}

// NewPanicError wraps a recovered panic value v in a PanicError carrying the
// calling goroutine's stack, so call it inside the deferred function that
// recovered. When v already is a *PanicError it is returned unchanged, so a
// panic that crosses several recovery layers keeps its original stack. It is
// for code that runs protocol callbacks on its own goroutine (a Transport
// wrapper, a Blocking-style adapter) and wants to report their panics the way
// the core does, through Loop.ReportPanic and the protocol's ConnectionLost.
func NewPanicError(v any) *PanicError {
	if pe, ok := v.(*PanicError); ok {
		return pe
	}
	buf := make([]byte, 16<<10)
	n := runtime.Stack(buf, false)
	return &PanicError{Value: v, Stack: buf[:n]}
}

// Error implements error.
func (e *PanicError) Error() string {
	return fmt.Sprintf("twine: panic: %v", e.Value)
}

// Unwrap returns the panic value when it was an error, so errors.Is works
// through PanicError.
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}
