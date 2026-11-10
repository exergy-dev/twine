package twine

import (
	"errors"
	"fmt"
)

// Sentinel errors. All are comparable with errors.Is.
var (
	// ErrClosed rejects a ConnectTCP or Dial Future when the connection was
	// closed during ConnectionMade (for example by Abort). Writes on a closed
	// transport do not return it: they are silent no-ops counted in
	// MetricsSnapshot.WriteAfterClose.
	ErrClosed = errors.New("twine: transport closed")

	// ErrLoopStopped reports work aimed at a loop that has exited. The core
	// itself reports ErrEngineStopped for ConnectTCP, Dial, DeferToThread
	// and Listen after Stop, and CallFromThread on an exited loop drops the
	// function (counted in MetricsSnapshot.BacklogDrops); protocol libraries
	// such as proto treat the two errors alike.
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
