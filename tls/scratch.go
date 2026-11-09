package tls

import (
	"runtime"
	"sync"
	"weak"

	"github.com/exergy-dev/twine"
)

// plainBufSize is the plaintext buffer handed to tls.Conn.Read: one
// maximum-size TLS record (2^14 bytes of plaintext).
const plainBufSize = 16 << 10

// loopScratch is the plaintext buffer that the TLS connections of one loop
// share for tls.Conn.Read. drain decrypts into it and lends the plaintext to
// inner.DataReceived, which may not retain it.
//
// Sharing is safe because a drain never starts on a loop while another one
// is using the buffer, except in the case busy handles. drain is entered
// from three places, each a top-level step of a loop iteration and never
// from inside another connection's callback:
//
//   - wrapper.DataReceived, which the core calls only from Loop.handleRead,
//     itself run from the poller's event dispatch or the loop's ready list;
//   - onHandshakeDone, a Future callback settled through CallFromThread,
//     that is, a loop task;
//   - drainTask, which ResumeReading schedules through CallFromThread. The
//     core's own ResumeReading only puts the connection on the ready list
//     (queueReady); it never reads synchronously.
//
// The exception is a protocol that feeds a TLS wrapper from inside another
// TLS connection's DataReceived: TLS nested inside TLS
// (Server(cfg, Server(cfg2, f))), or any hand-made adapter in the same
// position. The nested drain would decrypt into the buffer that holds the
// ciphertext it is reading from. (crypto/tls copies a record into its own
// buffer before decrypting it, and plaintext is shorter than ciphertext, so
// this mostly lands on bytes already consumed; nothing guarantees it, for
// example when a record straddles two chunks.) busy catches every such case:
// a drain that finds the buffer in use falls back to a buffer of its own
// connection, allocated on first use.
type loopScratch struct {
	buf  []byte
	busy bool // loop-only: a drain is using buf
}

var (
	scratchMu sync.Mutex
	// scratches is keyed by a weak pointer so that the map does not keep a
	// stopped engine's loops alive; a cleanup removes the entry once the
	// loop has been collected.
	scratches = map[weak.Pointer[twine.Loop]]*loopScratch{}
)

// scratchFor returns l's plaintext buffer, creating it on first use. It is
// called once per connection, when the handshake completes.
func scratchFor(l *twine.Loop) *loopScratch {
	k := weak.Make(l)
	scratchMu.Lock()
	defer scratchMu.Unlock()
	s := scratches[k]
	if s == nil {
		s = &loopScratch{buf: make([]byte, plainBufSize)}
		scratches[k] = s
		runtime.AddCleanup(l, func(k weak.Pointer[twine.Loop]) {
			scratchMu.Lock()
			delete(scratches, k)
			scratchMu.Unlock()
		}, k)
	}
	return s
}
