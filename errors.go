package twine

import (
	"errors"
	"fmt"
)

// Sentinel errors. All are comparable with errors.Is.
var (
	// ErrClosed is recorded when Write is called on a closed transport. The
	// write is a no-op; the error is counted in LoopMetrics.WriteAfterClose.
	ErrClosed = errors.New("twine: transport closed")

	// ErrLoopStopped is returned (or passed to callbacks) when CallFromThread,
	// CallLater or ConnectTCP is used after the loop has exited.
	ErrLoopStopped = errors.New("twine: loop stopped")

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

	// ErrEngineStopped rejects ConnectTCP and ListenTCP after Engine.Stop.
	ErrEngineStopped = errors.New("twine: engine stopped")

	// ErrLineTooLong aborts a LineReceiver connection whose line exceeds
	// MaxLineLength.
	ErrLineTooLong = errors.New("twine: line too long")

	// ErrFrameTooLarge aborts a framing connection whose frame exceeds the
	// configured maximum.
	ErrFrameTooLarge = errors.New("twine: frame too large")

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
