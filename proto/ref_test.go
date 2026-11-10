package proto

import (
	"bytes"
	"encoding/binary"
)

// Reference decoders: deliberately naive whole-buffer implementations of the
// framing rules, used as oracles by the unit and fuzz tests. Each returns the
// frames a streaming parser must deliver and the error it must abort with
// (nil when the input is a valid, possibly incomplete, stream).

func refLines(data, d []byte, max int) ([][]byte, error) {
	var out [][]byte
	for {
		i := bytes.Index(data, d)
		if i < 0 {
			break
		}
		if i > max {
			return out, ErrLineTooLong
		}
		out = append(out, data[:i])
		data = data[i+len(d):]
	}
	if len(data) > max+len(d)-1 {
		return out, ErrLineTooLong
	}
	return out, nil
}

func refNetstrings(data []byte, max int) ([][]byte, error) {
	var out [][]byte
	for len(data) > 0 {
		// Length digits.
		i := 0
		n := 0
		for i < len(data) && data[i] >= '0' && data[i] <= '9' {
			if i == 1 && data[0] == '0' {
				return out, ErrMalformedNetstring
			}
			n = n*10 + int(data[i]-'0')
			if n > max {
				return out, ErrFrameTooLarge
			}
			i++
		}
		if i == len(data) {
			return out, nil // incomplete length
		}
		if data[i] != ':' || i == 0 {
			return out, ErrMalformedNetstring
		}
		data = data[i+1:]
		if len(data) < n+1 {
			return out, nil // incomplete payload or comma
		}
		if data[n] != ',' {
			return out, ErrMalformedNetstring
		}
		out = append(out, data[:n])
		data = data[n+1:]
	}
	return out, nil
}

func refFrames(data []byte, prefix int, le, incl bool, max int) ([][]byte, error) {
	var out [][]byte
	for len(data) >= prefix {
		var n uint64
		switch prefix {
		case 1:
			n = uint64(data[0])
		case 2:
			if le {
				n = uint64(binary.LittleEndian.Uint16(data))
			} else {
				n = uint64(binary.BigEndian.Uint16(data))
			}
		default:
			if le {
				n = uint64(binary.LittleEndian.Uint32(data))
			} else {
				n = uint64(binary.BigEndian.Uint32(data))
			}
		}
		if incl {
			if n < uint64(prefix) {
				return out, ErrMalformedFrame
			}
			n -= uint64(prefix)
		}
		if n > uint64(max) {
			return out, ErrFrameTooLarge
		}
		data = data[prefix:]
		if uint64(len(data)) < n {
			return out, nil
		}
		out = append(out, data[:n])
		data = data[n:]
	}
	return out, nil
}

func framesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
