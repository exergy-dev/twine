package proto

import "errors"

// Framing errors specific to this package. ErrLineTooLong and
// ErrFrameTooLarge live in the twine package.
var (
	// ErrMalformedNetstring aborts a NetstringReceiver connection whose
	// input is not valid "len:data," framing (non-digit length, leading
	// zeros, empty length, or a missing trailing comma).
	ErrMalformedNetstring = errors.New("proto: malformed netstring")

	// ErrMalformedFrame aborts a LengthPrefixed connection whose length
	// prefix is invalid, i.e. smaller than the prefix itself when
	// LengthIncludesPrefix is set.
	ErrMalformedFrame = errors.New("proto: malformed length-prefixed frame")
)
