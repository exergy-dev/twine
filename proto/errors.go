package proto

import "errors"

// Framing errors. A framer that aborts records one of these and reports it
// to ConnectionLost (see the package doc).
var (
	// ErrLineTooLong aborts a LineReceiver connection whose line exceeds
	// MaxLineLength.
	ErrLineTooLong = errors.New("proto: line too long")

	// ErrFrameTooLarge aborts a NetstringReceiver, LengthPrefixed or AMP
	// connection whose frame or box exceeds the configured maximum.
	ErrFrameTooLarge = errors.New("proto: frame too large")

	// ErrMalformedNetstring aborts a NetstringReceiver connection whose
	// input is not valid "len:data," framing (non-digit length, leading
	// zeros, empty length, or a missing trailing comma).
	ErrMalformedNetstring = errors.New("proto: malformed netstring")

	// ErrMalformedFrame aborts a LengthPrefixed connection whose length
	// prefix is invalid, i.e. smaller than the prefix itself when
	// LengthIncludesPrefix is set.
	ErrMalformedFrame = errors.New("proto: malformed length-prefixed frame")
)

// ErrGaveUp is wrapped by the error passed to a ReconnectingClient's
// OnStateChange callback when the client gives up: after MaxRetries
// consecutive failures or when the engine stops.
var ErrGaveUp = errors.New("proto: reconnecting client gave up")
