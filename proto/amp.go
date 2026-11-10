package proto

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/exergy-dev/twine"
)

// Args is an AMP box's argument set: key/value pairs where both are byte
// strings (Go strings may hold arbitrary bytes). It is a map, as in Twisted
// where a box is a dict, so the order of pairs on the wire is unspecified.
type Args map[string]string

// AMP error codes, as defined by Twisted.
const (
	// UnknownErrorCode is sent when a handler fails with an error that is
	// not a *RemoteError.
	UnknownErrorCode = "UNKNOWN"
	// UnhandledErrorCode is sent when no handler accepts a command.
	UnhandledErrorCode = "UNHANDLED"
)

// Reserved AMP box keys (Twisted wire names).
const (
	ampAsk         = "_ask"
	ampAnswer      = "_answer"
	ampCommand     = "_command"
	ampError       = "_error"
	ampErrorCode   = "_error_code"
	ampErrorDesc   = "_error_description"
	ampMaxKeyLen   = 0xff
	ampMaxValueLen = 0xffff
)

// DefaultMaxBoxSize is the AMP limit on one box's encoded size used when
// MaxBoxSize is zero.
const DefaultMaxBoxSize = 1 << 20

// AMP errors.
var (
	// ErrRemote matches (errors.Is) every *RemoteError.
	ErrRemote = errors.New("proto: AMP remote error")
	// ErrUnhandledCommand matches a *RemoteError whose code is
	// UnhandledErrorCode: the peer has no handler for the command.
	ErrUnhandledCommand = errors.New("proto: AMP unhandled command")
	// ErrNotConnected rejects Call before ConnectionMade.
	ErrNotConnected = errors.New("proto: AMP not connected")
	// ErrConnectionLost rejects calls pending when the connection is lost,
	// and calls made afterwards. When the core reported an error it is
	// wrapped as well.
	ErrConnectionLost = errors.New("proto: AMP connection lost")
	// ErrBadArgs rejects a Call whose Args use an empty or reserved key
	// (_ask, _answer, _command, _error, _error_code, _error_description), a
	// key longer than 255 bytes or a value longer than 65535 bytes.
	ErrBadArgs = errors.New("proto: invalid AMP arguments")
)

// RemoteError is the rejection of a Call answered with an _error box. A
// handler may also fail with a *RemoteError to choose the code and
// description sent to the peer.
type RemoteError struct {
	// Code is the peer's _error_code (UnknownErrorCode, UnhandledErrorCode
	// or an application-defined code).
	Code string
	// Description is the peer's _error_description.
	Description string
}

// Error implements error.
func (e *RemoteError) Error() string {
	return fmt.Sprintf("proto: AMP remote error %s: %s", e.Code, e.Description)
}

// Is makes errors.Is(err, ErrRemote) true for every RemoteError and
// errors.Is(err, ErrUnhandledCommand) true for code UNHANDLED.
func (e *RemoteError) Is(target error) bool {
	return target == ErrRemote || (target == ErrUnhandledCommand && e.Code == UnhandledErrorCode)
}

// AMP implements Twisted's Asynchronous Messaging Protocol: request/response
// multiplexing with 32-bit ask ids. Embed it, set Handle, and use Call. A
// handler answers by calling reply, at once or later and from any
// goroutine, for example from the worker pool:
//
//	p.Handle = func(cmd string, args proto.Args, reply func(proto.Args, error)) {
//		switch cmd {
//		case "Echo": // answered at once
//			reply(args, nil)
//		case "Lookup": // blocking work runs on the worker pool
//			twine.DeferToThread(loop, func() (proto.Args, error) { return lookup(args) }).
//				Then(func(res proto.Args) { reply(res, nil) }).
//				Catch(func(err error) { reply(nil, err) })
//		default:
//			reply(nil, proto.ErrUnhandledCommand) // the peer gets an UNHANDLED error
//		}
//	}
//
// The wire format is Twisted's BinaryBoxProtocol, so a Twine AMP peer
// interoperates with twisted.protocols.amp.AMP: a box is a sequence of
// key/value strings, each with a 2-byte big-endian length prefix (framed by
// an internal LengthPrefixed with PrefixLen 2), terminated by an empty key.
// Keys are at most 255 bytes and values at most 65535. A request carries
// _command and, unless no answer is wanted, _ask (a hex counter); the reply
// carries _answer with the same tag, or _error with _error_code and
// _error_description.
//
// There is no schema layer: Twisted's Command classes and argument types
// (Integer, String, Unicode, Boolean, ...) have no counterpart, and values
// are the raw byte strings on the wire. To interoperate with a Twisted
// peer, encode values as its argument types do: an Integer is its decimal
// text (strconv.Itoa), a Boolean is "True" or "False", Unicode is UTF-8.
//
// Handler outcomes map to replies as follows: reply(res, nil) sends res as
// the answer; reply(nil, err) sends an _error box with the *RemoteError's
// code and description, UNHANDLED when err matches ErrUnhandledCommand, or
// UNKNOWN and err.Error() for any other error (use a *RemoteError to avoid
// exposing internal error text); a nil Handle sends UNHANDLED and a panic in
// Handle sends UNKNOWN. Requests without _ask get no reply, and reply is
// then a no-op.
//
// A box whose encoded size exceeds MaxBoxSize, or with a key longer than
// 255 bytes, aborts the connection with ErrFrameTooLarge.
type AMP struct {
	// Handle serves an incoming command. It runs on the loop; args excludes
	// the reserved keys. The handler answers by calling reply once, inside
	// Handle or later, from any goroutine: the reply is written on the
	// transport's loop. A second call is ignored, a reply after the
	// connection is lost writes nothing, and a handler that never calls
	// reply leaves the caller waiting (until its connection is lost). See
	// the type documentation for how res and err map to the reply box.
	Handle func(cmd string, args Args, reply func(res Args, err error))
	// OnConnectionMade, if set, is called from ConnectionMade after the
	// transport has been stored.
	OnConnectionMade func(t twine.Transport)
	// OnConnectionLost, if set, is called from ConnectionLost after pending
	// calls are rejected, with the framing error recorded before an abort or
	// the core's error otherwise.
	OnConnectionLost func(err error)
	// MaxBoxSize limits one incoming box's encoded size. Default 1 MiB.
	MaxBoxSize int

	lp      LengthPrefixed // holds the transport
	box     Args
	key     string
	haveKey bool
	boxSize int
	next    uint32
	pending map[uint32]twine.Resolver[Args]
	lost    error // non-nil once the connection is gone
	out     []byte
	hdr     [16]byte  // _ask pair of an outgoing request
	iov     [2][]byte // Writev argument; a literal through the interface would allocate
}

var _ twine.Protocol = (*AMP)(nil)

// ConnectionMade implements twine.Protocol.
func (a *AMP) ConnectionMade(t twine.Transport) {
	a.lp.PrefixLen = 2
	a.lp.MaxFrame = ampMaxValueLen
	a.lp.FrameReceived = a.frameReceived
	a.lp.ConnectionMade(t)
	if a.OnConnectionMade != nil {
		a.OnConnectionMade(t)
	}
}

// DataReceived implements twine.Protocol.
func (a *AMP) DataReceived(b []byte) { a.lp.DataReceived(b) }

// ConnectionLost implements twine.Protocol. Pending calls are rejected with
// ErrConnectionLost.
func (a *AMP) ConnectionLost(err error) {
	if a.lp.err != nil {
		err = a.lp.err
	}
	if err != nil {
		a.lost = fmt.Errorf("%w: %w", ErrConnectionLost, err)
	} else {
		a.lost = ErrConnectionLost
	}
	pending := a.pending
	a.pending = nil
	a.box = nil
	a.lp.buf = nil
	for _, r := range pending {
		r.Reject(a.lost)
	}
	if a.OnConnectionLost != nil {
		a.OnConnectionLost(err)
	}
}

// Transport returns the transport stored by ConnectionMade.
func (a *AMP) Transport() twine.Transport { return a.lp.t }

// Err returns the framing error that made the protocol abort, or nil.
func (a *AMP) Err() error { return a.lp.err }

// Pending returns the number of calls awaiting an answer. Loop-affine.
func (a *AMP) Pending() int { return len(a.pending) }

// onLoop runs fn on the transport's loop: inline when already there (or
// when there is no loop, as with test transports).
func (a *AMP) onLoop(fn func()) {
	l := a.lp.t.Loop()
	if l == nil || l.InLoop() {
		fn()
		return
	}
	l.CallFromThread(fn)
}

// loopGone reports whether the transport's loop has exited, in which case
// CallFromThread would drop work and a Future would never settle.
func (a *AMP) loopGone() bool {
	l := a.lp.t.Loop()
	return l != nil && l.Stopped()
}

// Call sends cmd with args and returns a Future for the answer. It may be
// called from any goroutine once ConnectionMade has happened-before the call
// (for example, when signalled from OnConnectionMade). args is encoded
// before Call returns, so the map may be reused at once; the request is
// written on the transport's loop and the Future's callbacks run there.
// The Future rejects with a
// *RemoteError (errors.Is ErrRemote) when the peer answers with an error,
// with ErrConnectionLost when the connection goes away first, with
// ErrNotConnected before ConnectionMade, and with ErrBadArgs for invalid
// args.
func (a *AMP) Call(cmd string, args Args) *twine.Future[Args] {
	if a.lp.t == nil {
		return twine.Rejected[Args](nil, ErrNotConnected)
	}
	fut, res := twine.NewFuture[Args](a.lp.t.Loop())
	body, err := encodeRequest(cmd, args)
	if err != nil {
		res.Reject(err)
		return fut
	}
	if a.loopGone() {
		res.Reject(ErrConnectionLost)
		return fut
	}
	a.onLoop(func() {
		if a.lost != nil {
			res.Reject(a.lost)
			return
		}
		if a.pending == nil {
			a.pending = make(map[uint32]twine.Resolver[Args])
		}
		var id uint32
		for {
			a.next++
			if a.next == 0 {
				continue
			}
			if _, busy := a.pending[a.next]; !busy {
				id = a.next
				break
			}
		}
		a.pending[id] = res
		a.writeRequest(id, body)
	})
	return fut
}

// CallNoAnswer sends cmd without an _ask key: the peer runs its handler but
// sends no reply. Like Call it may be used from any goroutine; it returns
// ErrBadArgs, ErrNotConnected or ErrConnectionLost (the last only when
// called on the loop) and is otherwise fire-and-forget.
func (a *AMP) CallNoAnswer(cmd string, args Args) error {
	if a.lp.t == nil {
		return ErrNotConnected
	}
	body, err := encodeRequest(cmd, args)
	if err != nil {
		return err
	}
	if l := a.lp.t.Loop(); (l == nil || l.InLoop()) && a.lost != nil {
		return a.lost
	}
	a.onLoop(func() {
		if a.lost == nil {
			a.lp.t.Write(body)
		}
	})
	return nil
}

// encodeRequest validates cmd and args and encodes the _command pair, every
// argument and the box terminator into a new buffer. It runs on the
// caller's goroutine, so the caller may reuse args as soon as Call returns.
func encodeRequest(cmd string, args Args) ([]byte, error) {
	n, err := validateArgs(cmd, args)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, 4+len(ampCommand)+len(cmd)+n+2)
	b = appendPair(b, ampCommand, cmd)
	for k, v := range args {
		b = appendPair(b, k, v)
	}
	return append(b, 0, 0), nil
}

// writeRequest writes the _ask pair for id and a body from encodeRequest
// with one Writev. Loop-affine.
func (a *AMP) writeRequest(id uint32, body []byte) {
	h := append(a.hdr[:0], 0, byte(len(ampAsk)))
	h = append(h, ampAsk...)
	h = append(h, 0, 0)
	n := len(h)
	h = strconv.AppendUint(h, uint64(id), 16)
	h[n-1] = byte(len(h) - n)
	a.iov[0], a.iov[1] = h, body
	a.lp.t.Writev(a.iov[:]...)
	a.iov = [2][]byte{}
}

// validateArgs checks cmd and args against the wire limits and returns the
// encoded size of args.
func validateArgs(cmd string, args Args) (int, error) {
	if len(cmd) > ampMaxValueLen {
		return 0, fmt.Errorf("%w: command name too long", ErrBadArgs)
	}
	n := 0
	for k, v := range args {
		switch {
		case k == "":
			return 0, fmt.Errorf("%w: empty key", ErrBadArgs)
		case len(k) > ampMaxKeyLen:
			return 0, fmt.Errorf("%w: key %.32q... longer than 255 bytes", ErrBadArgs, k)
		case len(v) > ampMaxValueLen:
			return 0, fmt.Errorf("%w: value of %q longer than 65535 bytes", ErrBadArgs, k)
		case isReserved(k):
			return 0, fmt.Errorf("%w: reserved key %q", ErrBadArgs, k)
		}
		n += 4 + len(k) + len(v)
	}
	return n, nil
}

func isReserved(k string) bool {
	switch k {
	case ampAsk, ampAnswer, ampCommand, ampError, ampErrorCode, ampErrorDesc:
		return true
	}
	return false
}

// send encodes one box (k1/v1 and k2/v2 when non-empty, then args) and
// writes it with a single Write, unless the connection is gone (a handler
// may answer after that). Loop-affine.
func (a *AMP) send(k1, v1, k2, v2 string, args Args) {
	if a.lost != nil {
		return
	}
	out := a.out[:0]
	out = appendPair(out, k1, v1)
	if k2 != "" {
		out = appendPair(out, k2, v2)
	}
	for k, v := range args {
		out = appendPair(out, k, v)
	}
	out = append(out, 0, 0)
	a.lp.t.Write(out)
	if cap(out) > retainLimit {
		out = nil
	}
	a.out = out
}

func appendPair(b []byte, k, v string) []byte {
	b = append(b, byte(len(k)>>8), byte(len(k)))
	b = append(b, k...)
	b = append(b, byte(len(v)>>8), byte(len(v)))
	return append(b, v...)
}

// frameReceived assembles boxes from the 2-byte-prefixed strings.
func (a *AMP) frameReceived(f []byte) {
	a.boxSize += 2 + len(f)
	if a.boxSize > a.maxBox() {
		a.lp.fail(ErrFrameTooLarge)
		return
	}
	if a.haveKey {
		if a.box == nil {
			a.box = make(Args)
		}
		a.box[a.key] = string(f)
		a.haveKey = false
		return
	}
	if len(f) == 0 {
		box := a.box
		a.box, a.boxSize = nil, 0
		a.dispatch(box)
		return
	}
	if len(f) > ampMaxKeyLen {
		a.lp.fail(ErrFrameTooLarge)
		return
	}
	a.key = internKey(f)
	a.haveKey = true
}

// internKey returns f as a string, without allocating for the reserved
// keys every request and answer carries.
func internKey(f []byte) string {
	switch string(f) {
	case ampAsk:
		return ampAsk
	case ampAnswer:
		return ampAnswer
	case ampCommand:
		return ampCommand
	case ampError:
		return ampError
	case ampErrorCode:
		return ampErrorCode
	case ampErrorDesc:
		return ampErrorDesc
	}
	return string(f)
}

func (a *AMP) maxBox() int {
	if a.MaxBoxSize <= 0 {
		return DefaultMaxBoxSize
	}
	return a.MaxBoxSize
}

func parseTag(s string) (uint32, bool) {
	v, err := strconv.ParseUint(s, 16, 32)
	return uint32(v), err == nil
}

// dispatch routes a complete box. Loop-affine.
func (a *AMP) dispatch(box Args) {
	if tag, ok := box[ampAnswer]; ok {
		delete(box, ampAnswer)
		if r := a.take(tag); r != nil {
			r.Resolve(box)
		}
		return
	}
	if tag, ok := box[ampError]; ok {
		if r := a.take(tag); r != nil {
			r.Reject(&RemoteError{Code: box[ampErrorCode], Description: box[ampErrorDesc]})
		}
		return
	}
	cmd, ok := box[ampCommand]
	if !ok {
		return // not a request, answer or error: ignore, as Twisted logs and drops
	}
	tag, hasAsk := box[ampAsk]
	delete(box, ampCommand)
	delete(box, ampAsk)
	if a.Handle == nil {
		if hasAsk {
			a.sendError(tag, unhandled(cmd))
		}
		return
	}
	reply := noReply
	if hasAsk {
		r := &ampReply{a: a, cmd: cmd, tag: tag}
		reply = r.reply
	}
	if perr := a.callHandler(cmd, box, reply); perr != nil {
		reply(nil, perr) // ignored if the handler replied before it panicked
	}
}

// callHandler runs Handle, converting a panic into an error.
func (a *AMP) callHandler(cmd string, args Args, reply func(Args, error)) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in AMP handler: %v", r)
		}
	}()
	a.Handle(cmd, args, reply)
	return nil
}

// noReply is the reply function of a request that wants no answer.
func noReply(Args, error) {}

// ampReply is the reply function of one request that carries _ask. It is
// a struct so that reply, a method value, is one allocation per request and
// the answer given off the loop travels to it without a closure.
type ampReply struct {
	a    *AMP
	cmd  string
	tag  string
	done atomic.Bool
	res  Args  // set by reply off the loop, read by deliver on it
	err  error // likewise
}

// reply answers the request once; later calls are ignored. Safe from any
// goroutine: on the loop it writes the reply at once, elsewhere it hops
// there with CallFromThread.
func (r *ampReply) reply(res Args, err error) {
	if !r.done.CompareAndSwap(false, true) {
		return
	}
	l := r.a.lp.t.Loop()
	if l == nil || l.InLoop() {
		r.a.answer(r.tag, r.cmd, res, err)
		return
	}
	r.res, r.err = res, err
	l.CallFromThread(r.deliver)
}

// deliver runs on the loop for a reply made elsewhere.
func (r *ampReply) deliver() { r.a.answer(r.tag, r.cmd, r.res, r.err) }

// answer writes the reply box for a handler outcome. Loop-affine.
func (a *AMP) answer(tag, cmd string, res Args, err error) {
	if err != nil {
		var re *RemoteError
		switch {
		case errors.As(err, &re):
		case errors.Is(err, ErrUnhandledCommand):
			re = unhandled(cmd)
		default:
			re = &RemoteError{Code: UnknownErrorCode, Description: err.Error()}
		}
		a.sendError(tag, re)
		return
	}
	if _, verr := validateArgs("", res); verr != nil {
		a.sendError(tag, &RemoteError{Code: UnknownErrorCode, Description: verr.Error()})
		return
	}
	a.send(ampAnswer, tag, "", "", res)
}

// unhandled is the error box for a command nobody serves.
func unhandled(cmd string) *RemoteError {
	return &RemoteError{Code: UnhandledErrorCode, Description: "Unhandled Command: " + strconv.Quote(cmd)}
}

func (a *AMP) sendError(tag string, re *RemoteError) {
	code := re.Code
	if code == "" {
		code = UnknownErrorCode
	}
	desc := re.Description
	if len(desc) > ampMaxValueLen {
		desc = desc[:ampMaxValueLen]
	}
	a.send(ampError, tag, ampErrorCode, code, Args{ampErrorDesc: desc})
}

func (a *AMP) take(tag string) twine.Resolver[Args] {
	id, ok := parseTag(tag)
	if !ok {
		return nil
	}
	r := a.pending[id]
	if r != nil {
		delete(a.pending, id)
	}
	return r
}
