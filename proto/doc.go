// Package proto is Twine's protocol library: embeddable framers and helpers
// modelled on Twisted's twisted.protocols.basic, twisted.protocols.amp and
// ReconnectingClientFactory.
//
// # Embedding pattern
//
// Every framer (LineReceiver, NetstringReceiver, LengthPrefixed, AMP) is a
// struct that implements twine.Protocol. Embed it in your protocol type and
// set its callback fields, usually in the Factory:
//
//	type chat struct {
//		proto.LineReceiver
//	}
//
//	func newChat(net.Addr) twine.Protocol {
//		c := &chat{}
//		c.LineReceived = c.lineReceived
//		c.OnConnectionLost = c.lost
//		return c
//	}
//
// The framer's ConnectionMade stores the transport (see Transport) and then
// calls the optional OnConnectionMade field; its ConnectionLost calls the
// optional OnConnectionLost field.
//
// # Framing errors
//
// The core delivers ConnectionLost(nil) after Transport.Abort. A framer that
// aborts because of a protocol violation (ErrLineTooLong, ErrFrameTooLarge,
// ErrMalformedNetstring, ErrMalformedFrame) records the error first and
// substitutes it in ConnectionLost, so OnConnectionLost observes the framing
// error rather than nil. The recorded error is also available from the
// framer's Err method. If the outer type defines its own ConnectionLost
// method, it shadows the framer's; call the embedded method
// (c.LineReceiver.ConnectionLost(err)) or consult Err() to keep the
// substitution.
//
// # Buffer lifetime
//
// Parsers work directly on the loop's shared read buffer and copy only an
// incomplete trailing frame into their own storage. The slice passed to a
// LineReceived, StringReceived or FrameReceived callback is valid only for
// the duration of the call; copy whatever you keep.
//
// # Threading
//
// Like every twine.Protocol, the framers are loop-affine: their methods and
// callbacks run on the owning loop. The exceptions are AMP.Call and
// AMP.CallNoAnswer, Broadcast, and ReconnectingClient (except its Transport
// method): they are safe to use from any goroutine and route work onto
// loops with CallFromThread.
package proto
