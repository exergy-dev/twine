package http

import "bytes"

// Parse results that are not HTTP status codes.
const (
	parseOK       = 0
	parseNeedMore = -1
)

// maxHeaders is the fixed capacity of the request header array. A request
// with more header lines is rejected with 431.
const maxHeaders = 64

// maxChunkLine bounds a chunk-size line (size plus extensions).
const maxChunkLine = 4096

// headerPair locates one request header inside request.data. Offsets are
// relative to the first byte of the request, so they stay valid when an
// incomplete request is moved from the loop buffer into the per-connection
// buffer. The struct is pointer-free, which keeps the 64-entry array out of
// the garbage collector's scan work.
type headerPair struct {
	ns, ne, vs, ve uint32
}

// request is the parsed form of one request head. All byte ranges are
// offsets into data.
type request struct {
	data []byte // the whole request (head and raw body) while it is served

	methodE      uint32
	uriS, uriE   uint32
	pathS, pathE uint32
	qS, qE       uint32
	verS         uint32
	minor        uint8
	hasQuery     bool
	pathSlash    bool // absolute-form target without a path: Path() is "/"
	absHost      bool // Host comes from an absolute-form request target
	hasHost      bool
	hostS, hostE uint32

	hdrs [maxHeaders]headerPair
	nh   int

	headLen       int
	contentLength int64 // -1 when absent
	clS, clE      uint32
	chunked       bool
	connClose     bool
	connKeepAlive bool
	expect100     bool
}

// tokenTable marks RFC 9110 tchar bytes.
var tokenTable = func() (t [256]bool) {
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		t[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		t[c] = true
	}
	return t
}()

// lowerTable maps ASCII upper case to lower case and every other byte to
// itself; it is the precomputed half of the case-insensitive compare.
var lowerTable = func() (t [256]byte) {
	for i := range t {
		c := byte(i)
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		t[i] = c
	}
	return t
}()

// text is the constraint of the helpers that take header names and values
// either as strings or as byte slices.
type text interface{ ~string | ~[]byte }

// equalFold reports whether a equals b under ASCII case folding.
func equalFold[A, B text](a A, b B) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if lowerTable[a[i]] != lowerTable[b[i]] {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func unhex(c byte) int64 {
	switch {
	case '0' <= c && c <= '9':
		return int64(c - '0')
	case 'a' <= c && c <= 'f':
		return int64(c-'a') + 10
	default:
		return int64(c-'A') + 10
	}
}

// slice returns data[s:e].
func (r *request) slice(s, e uint32) []byte { return r.data[s:e:e] }

// more is the result of running out of input: 431 when the head already
// exceeds the limit, otherwise a request for more data.
func more(tooBig bool) int {
	if tooBig {
		return 431
	}
	return parseNeedMore
}

// parseHead parses the request line and headers at the start of b. It
// returns the head length and parseOK, parseNeedMore, or the HTTP status
// code of the error response (400, 431, 501, 505). On parseOK r.data is set
// to b.
func (r *request) parseHead(b []byte, maxHdr int) (int, int) {
	r.reset()
	p := b
	tooBig := false
	if len(p) > maxHdr {
		p = p[:maxHdr]
		tooBig = true
	}
	i := 0

	// Method.
	for i < len(p) && tokenTable[p[i]] {
		i++
	}
	if i == len(p) {
		return 0, more(tooBig)
	}
	if p[i] != ' ' || i == 0 {
		return 0, 400
	}
	r.methodE = uint32(i)
	i++

	// Request target: visible ASCII and obs-text, no controls or spaces.
	s := i
	for i < len(p) && p[i] > ' ' && p[i] != 0x7f {
		i++
	}
	if i == len(p) {
		return 0, more(tooBig)
	}
	if p[i] != ' ' || i == s {
		return 0, 400
	}
	r.uriS, r.uriE = uint32(s), uint32(i)
	if !r.splitTarget(p, s, i) {
		return 0, 400
	}
	i++

	// Version: exactly "HTTP/1.0" or "HTTP/1.1" followed by CRLF.
	r.verS = uint32(i)
	if len(p)-i < 10 {
		if !versionPrefix(p[i:]) {
			return 0, 400
		}
		return 0, more(tooBig)
	}
	v := p[i : i+10]
	if string(v[:7]) != "HTTP/1." || (v[7] != '0' && v[7] != '1') || v[8] != '\r' || v[9] != '\n' {
		if versionPrefix(v) {
			return 0, 505 // well formed, unsupported
		}
		return 0, 400
	}
	r.minor = v[7] - '0'
	i += 10

	// Header fields.
	for {
		if i >= len(p) {
			return 0, more(tooBig)
		}
		if p[i] == '\r' {
			if i+1 >= len(p) {
				return 0, more(tooBig)
			}
			if p[i+1] != '\n' {
				return 0, 400
			}
			i += 2
			break
		}
		ns := i
		for i < len(p) && tokenTable[p[i]] {
			i++
		}
		if i == len(p) {
			return 0, more(tooBig)
		}
		// Rejects obs-fold (a line starting with whitespace), whitespace
		// before the colon, bare LF and any other non-token byte.
		if p[i] != ':' || i == ns {
			return 0, 400
		}
		ne := i
		i++
		for i < len(p) && (p[i] == ' ' || p[i] == '\t') {
			i++
		}
		vs := i
		for i < len(p) && p[i] != '\r' {
			c := p[i]
			if c < ' ' && c != '\t' || c == 0x7f {
				return 0, 400
			}
			i++
		}
		if i+1 >= len(p) {
			return 0, more(tooBig)
		}
		if p[i+1] != '\n' {
			return 0, 400
		}
		ve := i
		for ve > vs && (p[ve-1] == ' ' || p[ve-1] == '\t') {
			ve--
		}
		i += 2
		if r.nh == maxHeaders {
			return 0, 431
		}
		h := headerPair{uint32(ns), uint32(ne), uint32(vs), uint32(ve)}
		r.hdrs[r.nh] = h
		r.nh++
		if code := r.classify(p, h); code != parseOK {
			return 0, code
		}
	}

	// Message framing and mandatory fields.
	if r.chunked && r.contentLength >= 0 {
		return 0, 400 // request smuggling vector; refuse rather than guess
	}
	if r.chunked && r.minor == 0 {
		return 0, 400
	}
	if r.minor == 1 && !r.hasHost {
		return 0, 400
	}
	r.headLen = i
	r.data = b
	return i, parseOK
}

// versionPrefix reports whether v is consistent with "HTTP/d.d\r\n" for
// as many bytes as it has (at most 10 are checked).
func versionPrefix(v []byte) bool {
	const pat = "HTTP/0.0\r\n"
	for j := 0; j < len(v) && j < len(pat); j++ {
		c := v[j]
		if pat[j] == '0' {
			if c < '0' || c > '9' {
				return false
			}
		} else if c != pat[j] {
			return false
		}
	}
	return true
}

// reset clears the parsed state for a new request.
func (r *request) reset() {
	r.data = nil
	r.methodE, r.uriS, r.uriE = 0, 0, 0
	r.pathS, r.pathE, r.qS, r.qE = 0, 0, 0, 0
	r.verS, r.minor = 0, 0
	r.hasQuery, r.pathSlash, r.absHost, r.hasHost = false, false, false, false
	r.hostS, r.hostE = 0, 0
	r.nh = 0
	r.headLen = 0
	r.contentLength = -1
	r.clS, r.clE = 0, 0
	r.chunked, r.connClose, r.connKeepAlive, r.expect100 = false, false, false, false
}

// splitTarget validates the request target p[s:e] and records the path and
// query ranges. Supported forms: origin-form ("/path?q"), asterisk-form
// ("*") and absolute-form ("http://host/path?q"). Percent escapes in the
// path must be well formed.
func (r *request) splitTarget(p []byte, s, e int) bool {
	t := p[s:e]
	switch {
	case t[0] == '/':
		// origin-form
	case len(t) == 1 && t[0] == '*':
		r.pathS, r.pathE = uint32(s), uint32(e)
		return true
	default:
		n := 0
		if len(t) >= 7 && equalFold(t[:7], "http://") {
			n = 7
		} else if len(t) >= 8 && equalFold(t[:8], "https://") {
			n = 8
		} else {
			return false
		}
		// Authority: a conservative host[:port] (no userinfo, no IP-literal).
		hs := n
		for n < len(t) && t[n] != '/' && t[n] != '?' {
			n++
		}
		if !validAuthority(t[hs:n]) {
			return false
		}
		r.absHost = true
		r.hostS, r.hostE = uint32(s+hs), uint32(s+n)
		if n == len(t) || t[n] == '?' {
			r.pathSlash = true
			r.pathS, r.pathE = uint32(s+n), uint32(s+n)
			if n < len(t) {
				r.hasQuery = true
				r.qS, r.qE = uint32(s+n+1), uint32(e)
			}
			return true
		}
		s += n
		t = t[n:]
	}
	q := bytes.IndexByte(t, '?')
	path := t
	if q >= 0 {
		path = t[:q]
		r.hasQuery = true
		r.qS, r.qE = uint32(s+q+1), uint32(e)
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '%' {
			if i+2 >= len(path) || !isHex(path[i+1]) || !isHex(path[i+2]) {
				return false
			}
			i += 2
		}
	}
	r.pathS, r.pathE = uint32(s), uint32(s+len(path))
	return true
}

// validAuthority accepts reg-name hosts made of letters, digits, '.', '-'
// and '_' with an optional numeric port.
func validAuthority(a []byte) bool {
	if len(a) == 0 {
		return false
	}
	i := 0
	for i < len(a) {
		c := a[i]
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '-' || c == '_' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return false
	}
	if i == len(a) {
		return true
	}
	if a[i] != ':' {
		return false
	}
	for i++; i < len(a); i++ {
		if a[i] < '0' || a[i] > '9' {
			return false
		}
	}
	return true
}

// classify records the framing and connection-management headers.
func (r *request) classify(p []byte, h headerPair) int {
	name := p[h.ns:h.ne]
	val := p[h.vs:h.ve]
	switch len(name) {
	case 4:
		if equalFold(name, "host") {
			if r.hasHost {
				return 400
			}
			r.hasHost = true
			if !r.absHost {
				r.hostS, r.hostE = h.vs, h.ve
			}
		}
	case 6:
		if equalFold(name, "expect") && equalFold(val, "100-continue") {
			r.expect100 = true
		}
	case 10:
		if equalFold(name, "connection") {
			r.parseConnection(val)
		}
	case 14:
		if equalFold(name, "content-length") {
			if len(val) == 0 || len(val) > 18 {
				return 400
			}
			var n int64
			for _, c := range val {
				if c < '0' || c > '9' {
					return 400
				}
				n = n*10 + int64(c-'0')
			}
			if r.contentLength >= 0 {
				// Repeated Content-Length must be byte-identical.
				if string(p[r.clS:r.clE]) != string(val) {
					return 400
				}
				return parseOK
			}
			r.contentLength = n
			r.clS, r.clE = h.vs, h.ve
		}
	case 17:
		if equalFold(name, "transfer-encoding") {
			if r.chunked {
				return 400
			}
			if !equalFold(val, "chunked") {
				return 501
			}
			r.chunked = true
		}
	}
	return parseOK
}

// parseConnection scans a comma-separated Connection token list.
func (r *request) parseConnection(v []byte) {
	for len(v) > 0 {
		i := bytes.IndexByte(v, ',')
		tok := v
		if i >= 0 {
			tok, v = v[:i], v[i+1:]
		} else {
			v = nil
		}
		for len(tok) > 0 && (tok[0] == ' ' || tok[0] == '\t') {
			tok = tok[1:]
		}
		for len(tok) > 0 && (tok[len(tok)-1] == ' ' || tok[len(tok)-1] == '\t') {
			tok = tok[:len(tok)-1]
		}
		switch {
		case equalFold(tok, "close"):
			r.connClose = true
		case equalFold(tok, "keep-alive"):
			r.connKeepAlive = true
		}
	}
}

// chunkState is the incremental chunked-body decoder. Offsets are relative
// to the start of the request so the raw bytes may move between the loop
// buffer and the per-connection buffer between calls.
type chunkState struct {
	off     int   // next undecoded raw byte
	phase   uint8 // chunkSize, chunkData, chunkCRLF, chunkTrailer
	remain  int64 // bytes left in the current chunk
	trailer int   // trailer bytes seen
}

const (
	chunkSize uint8 = iota
	chunkData
	chunkCRLF
	chunkTrailer
)

// decodeChunked continues decoding the chunked body of the request held in
// data (data[0] is the first byte of the request) and appends payload bytes
// to *body. It returns the total request length and parseOK when the body is
// complete, parseNeedMore, or an error status.
func (cs *chunkState) decode(data []byte, body *[]byte, maxBody, maxHdr int) (int, int) {
	for {
		switch cs.phase {
		case chunkSize:
			nl := bytes.IndexByte(data[cs.off:], '\n')
			if nl < 0 {
				if len(data)-cs.off > maxChunkLine {
					return 0, 400
				}
				return 0, parseNeedMore
			}
			line := data[cs.off : cs.off+nl]
			if nl > maxChunkLine || len(line) == 0 || line[len(line)-1] != '\r' {
				return 0, 400
			}
			line = line[:len(line)-1]
			var n int64
			j := 0
			for ; j < len(line) && isHex(line[j]); j++ {
				if j == 15 {
					return 0, 400
				}
				n = n<<4 | unhex(line[j])
			}
			if j == 0 {
				return 0, 400
			}
			if j < len(line) {
				if line[j] != ';' {
					return 0, 400
				}
				for _, c := range line[j+1:] {
					if c < ' ' && c != '\t' || c == 0x7f {
						return 0, 400
					}
				}
			}
			cs.off += nl + 1
			if n == 0 {
				cs.phase = chunkTrailer
				continue
			}
			if int64(len(*body))+n > int64(maxBody) {
				return 0, 413
			}
			cs.remain = n
			cs.phase = chunkData
		case chunkData:
			avail := int64(len(data) - cs.off)
			if avail == 0 {
				return 0, parseNeedMore
			}
			n := min(avail, cs.remain)
			*body = append(*body, data[cs.off:cs.off+int(n)]...)
			cs.off += int(n)
			cs.remain -= n
			if cs.remain > 0 {
				return 0, parseNeedMore
			}
			cs.phase = chunkCRLF
		case chunkCRLF:
			if len(data)-cs.off < 2 {
				if len(data)-cs.off == 1 && data[cs.off] != '\r' {
					return 0, 400
				}
				return 0, parseNeedMore
			}
			if data[cs.off] != '\r' || data[cs.off+1] != '\n' {
				return 0, 400
			}
			cs.off += 2
			cs.phase = chunkSize
		case chunkTrailer:
			nl := bytes.IndexByte(data[cs.off:], '\n')
			if nl < 0 {
				if cs.trailer+len(data)-cs.off > maxHdr {
					return 0, 431
				}
				return 0, parseNeedMore
			}
			line := data[cs.off : cs.off+nl]
			if len(line) == 0 || line[len(line)-1] != '\r' {
				return 0, 400
			}
			line = line[:len(line)-1]
			cs.off += nl + 1
			if len(line) == 0 {
				return cs.off, parseOK
			}
			cs.trailer += nl + 1
			if cs.trailer > maxHdr {
				return 0, 431
			}
			if !validTrailer(line) {
				return 0, 400
			}
		}
	}
}

// validTrailer checks "token: value" syntax for a trailer field. Trailer
// fields are validated and then discarded.
func validTrailer(line []byte) bool {
	i := 0
	for i < len(line) && tokenTable[line[i]] {
		i++
	}
	if i == 0 || i == len(line) || line[i] != ':' {
		return false
	}
	for _, c := range line[i+1:] {
		if c < ' ' && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
}
