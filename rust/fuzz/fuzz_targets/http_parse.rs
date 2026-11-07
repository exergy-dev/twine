#![no_main]
//! parse_request / ChunkState.
//!
//! First input byte selects the mode: even = structured request built from an
//! `Arbitrary` description (must parse, fields must round-trip, every proper
//! prefix must be Incomplete); odd = arbitrary wire bytes behind the common
//! header (never panics, internal consistency, prefix/chunk invariance,
//! ChunkState against a reference decoder).

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use twine_fuzz::{chunk_list, params};
use twine_http::{parse_request, ChunkState, Limits, ParseError, Parsed, MAX_HEADERS};

// ---------------------------------------------------------------- raw mode

fn check_parsed(data: &[u8], limits: &Limits, r: &Result<Parsed, ParseError>) {
    let Ok(Parsed::Complete { head_len, request }) = r else {
        return;
    };
    let head_len = *head_len;
    assert!(head_len <= data.len(), "head_len beyond input");
    assert!(head_len <= limits.max_header_bytes, "head over limit");
    assert_eq!(head_len, request.head_len());
    assert!(request.num_headers() <= MAX_HEADERS);
    for h in request.headers() {
        assert!(h.name_start < h.name_end, "empty header name");
        assert!(h.name_end <= h.value_start, "value before name end");
        assert!(h.value_start <= h.value_end);
        assert!(h.value_end as usize <= head_len, "header offset outside head");
    }
    // Accessors must not panic (they slice with the recorded offsets).
    let d = &data[..head_len];
    let _ = request.method(d);
    let uri = request.request_uri(d);
    let path = request.path(d);
    let _ = request.raw_query(d);
    let _ = request.version(d);
    let _ = request.host(d);
    let _ = request.header(d, b"content-length");
    assert!(!request.method(d).is_empty() && !uri.is_empty() && !path.is_empty());
    assert!(request.minor() <= 1);
    if let Some(cl) = request.content_length() {
        assert!(cl <= limits.max_body_bytes as u64);
    }
    // Determinism / self-containment: the head alone parses identically.
    assert_eq!(&parse_request(d, limits), r);
    // Any proper prefix of a complete head is Incomplete.
    let step = (head_len / 48).max(1);
    for k in (0..head_len).step_by(step).chain([head_len.saturating_sub(1)]) {
        if k < head_len {
            assert_eq!(
                parse_request(&data[..k], limits),
                Ok(Parsed::Incomplete),
                "prefix {k} of a complete head is not Incomplete"
            );
        }
    }
}

#[derive(Debug, PartialEq)]
enum RefChunk {
    Done(usize, Vec<u8>),
    Incomplete,
    Err(ParseError),
}

/// One-shot reference decoder for a chunked body starting at `start`.
fn ref_chunked(data: &[u8], start: usize, max_body: usize, max_hdr: usize) -> RefChunk {
    use ParseError::*;
    let mut pos = start;
    let mut body = Vec::new();
    loop {
        // chunk-size line
        let rest = &data[pos..];
        let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
            return if rest.len() > 4096 { RefChunk::Err(BadRequest) } else { RefChunk::Incomplete };
        };
        if nl > 4096 || nl == 0 || rest[nl - 1] != b'\r' {
            return RefChunk::Err(BadRequest);
        }
        let line = &rest[..nl - 1];
        let hex = line.iter().take_while(|c| c.is_ascii_hexdigit()).count();
        // Leading zeros carry no value; > 15 significant digits overflow.
        let zeros = line[..hex].iter().take_while(|&&c| c == b'0').count();
        if hex == 0 || hex - zeros > 15 {
            return RefChunk::Err(BadRequest);
        }
        let size = if hex == zeros {
            0
        } else {
            usize::from_str_radix(std::str::from_utf8(&line[zeros..hex]).unwrap(), 16).unwrap()
        };
        if hex < line.len() {
            if line[hex] != b';' || line[hex + 1..].iter().any(|&c| (c < b' ' && c != b'\t') || c == 0x7f) {
                return RefChunk::Err(BadRequest);
            }
        }
        pos += nl + 1;
        if size == 0 {
            break;
        }
        if body.len() + size > max_body {
            return RefChunk::Err(PayloadTooLarge);
        }
        let rest = &data[pos..];
        let take = size.min(rest.len());
        body.extend_from_slice(&rest[..take]);
        pos += take;
        if take < size {
            return RefChunk::Incomplete;
        }
        match &data[pos..] {
            [] => return RefChunk::Incomplete,
            [c] => return if *c == b'\r' { RefChunk::Incomplete } else { RefChunk::Err(BadRequest) },
            [b'\r', b'\n', ..] => pos += 2,
            _ => return RefChunk::Err(BadRequest),
        }
    }
    // trailers
    let mut trailer = 0usize;
    loop {
        let rest = &data[pos..];
        let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
            // A trailing CR may start the final blank line: not counted yet.
            let pending = rest.strip_suffix(b"\r").unwrap_or(rest).len();
            return if trailer + pending > max_hdr { RefChunk::Err(HeaderFieldsTooLarge) } else { RefChunk::Incomplete };
        };
        // Size is checked before syntax so the error is split-independent.
        if &rest[..nl] != b"\r" && trailer + rest[..nl].strip_suffix(b"\r").unwrap_or(&rest[..nl]).len() > max_hdr {
            return RefChunk::Err(HeaderFieldsTooLarge);
        }
        if nl == 0 || rest[nl - 1] != b'\r' {
            return RefChunk::Err(BadRequest);
        }
        let line = &rest[..nl - 1];
        pos += nl + 1;
        if line.is_empty() {
            return RefChunk::Done(pos, body);
        }
        trailer += nl + 1;
        if trailer > max_hdr {
            return RefChunk::Err(HeaderFieldsTooLarge);
        }
        let name = line.iter().take_while(|&&c| is_tchar(c)).count();
        if name == 0 || name == line.len() || line[name] != b':' {
            return RefChunk::Err(BadRequest);
        }
        if line[name + 1..].iter().any(|&c| (c < b' ' && c != b'\t') || c == 0x7f) {
            return RefChunk::Err(BadRequest);
        }
    }
}

fn is_tchar(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c)
}

fn real_chunked(data: &[u8], start: usize, max_body: usize, max_hdr: usize) -> RefChunk {
    let mut body = Vec::new();
    match ChunkState::new(start).decode(data, &mut body, max_body, max_hdr) {
        Ok(Some(n)) => RefChunk::Done(n, body),
        Ok(None) => RefChunk::Incomplete,
        Err(e) => RefChunk::Err(e),
    }
}

fn raw_mode(input: &[u8]) {
    let Some(p) = params(input) else { return };
    let data = p.data;
    let limits = if p.flags & 1 == 0 {
        Limits::default()
    } else {
        Limits {
            max_header_bytes: p.limit().min(1024),
            max_body_bytes: p.sel as usize,
        }
    };

    // parse_request: never panics, internally consistent.
    let full = parse_request(data, &limits);
    check_parsed(data, &limits, &full);

    // Incremental feeding at chunk boundaries (and around them).
    let mut cuts: Vec<usize> = Vec::new();
    let mut at = 0;
    for c in chunk_list(data, p.splits) {
        at += c.len();
        cuts.extend([at.saturating_sub(1), at]);
    }
    for k in cuts {
        let r = parse_request(&data[..k], &limits);
        match &full {
            Ok(Parsed::Complete { head_len, .. }) if k < *head_len => {
                assert_eq!(r, Ok(Parsed::Incomplete), "prefix {k} of complete head")
            }
            Ok(Parsed::Complete { .. }) => assert_eq!(r, full, "prefix {k} past head"),
            Ok(Parsed::Incomplete) => assert_eq!(r, Ok(Parsed::Incomplete), "prefix of incomplete"),
            Err(e) => assert!(
                r == Ok(Parsed::Incomplete) || r == Err(*e),
                "prefix {k}: {r:?} vs full error {e:?}"
            ),
        }
    }

    // ChunkState vs reference, from offset 0 and from the head end.
    let max_body = p.sel as usize * 8;
    let max_hdr = p.limit().min(256);
    let mut starts = vec![0usize];
    if let Ok(Parsed::Complete { head_len, .. }) = &full {
        starts.push(*head_len);
    }
    for s in starts {
        let want = ref_chunked(data, s, max_body, max_hdr);
        assert_eq!(real_chunked(data, s, max_body, max_hdr), want, "ChunkState vs reference");

        // Incremental: one state, growing buffer, at chunk boundaries.
        let mut st = ChunkState::new(s);
        let mut body = Vec::new();
        let mut at = 0;
        let mut last = Ok(None);
        for c in chunk_list(data, p.splits) {
            at += c.len();
            if at < s {
                continue;
            }
            last = st.decode(&data[..at], &mut body, max_body, max_hdr);
            if !matches!(last, Ok(None)) {
                break;
            }
        }
        match (&last, &want) {
            (Ok(Some(n)), RefChunk::Done(m, b)) => {
                assert_eq!(n, m);
                assert_eq!(&body, b);
            }
            // Both reject. The code may differ: a partial line that already
            // exceeds a limit is reported before a later byte shows the
            // line is malformed (431 vs 400).
            (Err(_), RefChunk::Err(_)) => {}
            (Ok(None), RefChunk::Incomplete) => {}
            // Fed in one go only up to the last boundary: nothing left over.
            (l, w) => panic!("incremental ChunkState {l:?} vs one-shot reference {w:?}"),
        }
    }
}

// ----------------------------------------------------------- structured mode

const NAME: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.";
const VALUE: &[u8] = b"abcdefXYZ 0189-_./:;=,*()[]{}<>@!\"#$&'+?~\t";
const PATH: &[u8] = b"abcdefXYZ0189-_./~!$&'()*+,;=:@";

fn pick(src: &[u8], alphabet: &[u8], min: usize, max: usize) -> Vec<u8> {
    let mut v: Vec<u8> = src.iter().take(max).map(|b| alphabet[*b as usize % alphabet.len()]).collect();
    while v.len() < min {
        v.push(alphabet[0]);
    }
    v
}

#[derive(Arbitrary, Debug)]
enum Target {
    Origin { path: Vec<u8>, query: Option<Vec<u8>>, pct: Option<u8> },
    Absolute { https: bool, host: Vec<u8>, port: Option<u16>, path: Option<Vec<u8>>, query: Option<Vec<u8>> },
    Asterisk,
}

#[derive(Arbitrary, Debug)]
enum Body {
    None,
    Length(Vec<u8>),
    Chunked { chunks: Vec<Vec<u8>>, ext: Vec<Option<Vec<u8>>>, trailers: Vec<(Vec<u8>, Vec<u8>)> },
}

#[derive(Arbitrary, Debug)]
struct Case {
    method: u8,
    custom_method: Vec<u8>,
    target: Target,
    http10: bool,
    headers: Vec<(Vec<u8>, Vec<u8>, bool, bool)>,
    host: Vec<u8>,
    host_header: bool,
    conn: u8,
    dup_cl: bool,
    body: Body,
}

fn structured(input: &[u8]) {
    let mut u = Unstructured::new(input);
    let Ok(c) = Case::arbitrary(&mut u) else { return };

    let method: Vec<u8> = match c.method % 8 {
        0 => b"GET".to_vec(),
        1 => b"POST".to_vec(),
        2 => b"PUT".to_vec(),
        3 => b"DELETE".to_vec(),
        4 => b"HEAD".to_vec(),
        5 => b"OPTIONS".to_vec(),
        6 => b"PATCH".to_vec(),
        _ => pick(&c.custom_method, &NAME[..52], 1, 20),
    };
    let chunked = matches!(c.body, Body::Chunked { .. });
    let http10 = c.http10 && !chunked;

    // Request target and expected path/query/host.
    let (target, e_path, e_query, mut e_host): (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>) = match &c.target {
        Target::Origin { path, query, pct } => {
            let mut p = vec![b'/'];
            p.extend(pick(path, PATH, 0, 40));
            if let Some(b) = pct {
                p.extend(format!("%{:02X}", b).bytes());
            }
            let q = query.as_ref().map(|q| pick(q, PATH, 0, 30));
            let mut t = p.clone();
            if let Some(q) = &q {
                t.push(b'?');
                t.extend(q);
            }
            (t, p, q, None)
        }
        Target::Absolute { https, host, port, path, query } => {
            let h = pick(host, &NAME[..62], 1, 20);
            let mut auth = h.clone();
            if let Some(pt) = port {
                auth.extend(format!(":{pt}").bytes());
            }
            let mut t = if *https { b"https://".to_vec() } else { b"http://".to_vec() };
            t.extend(&auth);
            let p = path.as_ref().map(|p| {
                let mut v = vec![b'/'];
                v.extend(pick(p, PATH, 0, 30));
                v
            });
            if let Some(p) = &p {
                t.extend(p);
            }
            let q = query.as_ref().map(|q| pick(q, PATH, 0, 30));
            if let Some(q) = &q {
                t.push(b'?');
                t.extend(q);
            }
            (t, p.unwrap_or_else(|| b"/".to_vec()), q, Some(auth))
        }
        Target::Asterisk => (b"*".to_vec(), b"*".to_vec(), None, None),
    };

    let mut head = Vec::new();
    head.extend(&method);
    head.push(b' ');
    head.extend(&target);
    head.extend(if http10 { &b" HTTP/1.0\r\n"[..] } else { &b" HTTP/1.1\r\n"[..] });

    let mut expect: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut emit = |head: &mut Vec<u8>, name: Vec<u8>, lead: &[u8], value: Vec<u8>, trail: &[u8]| {
        head.extend(&name);
        head.push(b':');
        head.extend(lead);
        head.extend(&value);
        head.extend(trail);
        head.extend(b"\r\n");
        expect.push((name, value));
    };

    for (n, v, lead, trail) in c.headers.iter().take(MAX_HEADERS - 6) {
        let mut name = b"X-".to_vec();
        name.extend(pick(n, NAME, 1, 20));
        let v = pick(v, VALUE, 0, 40);
        let v = v.trim_ascii().to_vec();
        emit(&mut head, name, if *lead { b" \t " } else { b" " }, v, if *trail { b" \t" } else { b"" });
    }
    let need_host = !http10 || c.host_header;
    if need_host {
        let h = pick(&c.host, &NAME[..62], 1, 20);
        if e_host.is_none() {
            e_host = Some(h.clone());
        }
        emit(&mut head, b"Host".to_vec(), b" ", h, b"");
    }
    let (mut e_close, mut e_ka) = (false, false);
    match c.conn % 4 {
        1 => {
            e_close = true;
            emit(&mut head, b"Connection".to_vec(), b" ", b"close".to_vec(), b"");
        }
        2 => {
            e_ka = true;
            emit(&mut head, b"connection".to_vec(), b" ", b"Keep-Alive".to_vec(), b"");
        }
        3 => {
            e_close = true;
            e_ka = true;
            emit(&mut head, b"Connection".to_vec(), b" ", b"keep-alive, Close".to_vec(), b"");
        }
        _ => {}
    }
    let mut payload = Vec::new();
    let mut tail = Vec::new();
    let mut e_cl = None;
    match &c.body {
        Body::None => {}
        Body::Length(b) => {
            let b = &b[..b.len().min(2000)];
            let l = b.len().to_string().into_bytes();
            emit(&mut head, b"Content-Length".to_vec(), b" ", l.clone(), b"");
            if c.dup_cl {
                emit(&mut head, b"content-length".to_vec(), b" ", l, b"");
            }
            e_cl = Some(b.len() as u64);
            payload = b.to_vec();
            tail = b.to_vec();
        }
        Body::Chunked { chunks, ext, trailers } => {
            emit(&mut head, b"Transfer-Encoding".to_vec(), b" ", b"Chunked".to_vec(), b"");
            for (i, ch) in chunks.iter().take(12).enumerate() {
                if ch.is_empty() {
                    continue; // a zero-size chunk would terminate the body
                }
                let ch = &ch[..ch.len().min(300)];
                tail.extend(format!("{:x}", ch.len()).bytes());
                if let Some(Some(x)) = ext.get(i) {
                    tail.push(b';');
                    tail.extend(pick(x, NAME, 1, 10));
                }
                tail.extend(b"\r\n");
                tail.extend(ch);
                tail.extend(b"\r\n");
                payload.extend(ch);
            }
            tail.extend(b"0\r\n");
            for (n, v) in trailers.iter().take(4) {
                tail.extend(pick(n, NAME, 1, 10));
                tail.extend(b": ");
                tail.extend(pick(v, VALUE, 0, 20));
                tail.extend(b"\r\n");
            }
            tail.extend(b"\r\n");
        }
    }
    head.extend(b"\r\n");
    let mut input = head.clone();
    input.extend(&tail);

    let limits = Limits::default();
    let one = parse_request(&input, &limits);
    let (head_len, req) = match &one {
        Ok(Parsed::Complete { head_len, request }) => (*head_len, request),
        other => panic!("structured request not accepted: {other:?}\n{:?}", String::from_utf8_lossy(&input)),
    };
    assert_eq!(head_len, head.len());
    assert_eq!(req.method(&input), &method[..]);
    assert_eq!(req.request_uri(&input), &target[..]);
    assert_eq!(req.path(&input), &e_path[..]);
    assert_eq!(req.has_query(), e_query.is_some());
    assert_eq!(req.raw_query(&input), e_query.as_deref().unwrap_or(b""));
    assert_eq!(req.minor(), if http10 { 0 } else { 1 });
    assert_eq!(req.version(&input), if http10 { &b"HTTP/1.0"[..] } else { &b"HTTP/1.1"[..] });
    assert_eq!(req.host(&input), e_host.as_deref());
    assert_eq!(req.conn_close(), e_close);
    assert_eq!(req.conn_keep_alive(), e_ka);
    assert_eq!(req.content_length(), e_cl);
    assert_eq!(req.is_chunked(), chunked);
    let got: Vec<(&[u8], &[u8])> = req
        .headers()
        .iter()
        .map(|h| (&input[h.name_start as usize..h.name_end as usize], &input[h.value_start as usize..h.value_end as usize]))
        .collect();
    let want: Vec<(&[u8], &[u8])> = expect.iter().map(|(n, v)| (&n[..], &v[..])).collect();
    assert_eq!(got, want, "header list");

    // Incremental: every proper prefix of the head is Incomplete (sampled for
    // long heads); every longer prefix is Complete with the same fields.
    let step = (head_len / 200).max(1);
    for k in (0..head_len).step_by(step).chain([head_len - 1]) {
        assert_eq!(parse_request(&input[..k], &limits), Ok(Parsed::Incomplete), "prefix {k}");
    }
    let step = ((input.len() - head_len) / 50).max(1);
    for k in (head_len..=input.len()).step_by(step).chain([input.len()]) {
        assert_eq!(parse_request(&input[..k], &limits), one, "prefix {k} past head");
    }

    // Bodies.
    if chunked {
        let (mb, mh) = (limits.max_body_bytes, limits.max_header_bytes);
        let want = RefChunk::Done(input.len(), payload.clone());
        assert_eq!(ref_chunked(&input, head_len, mb, mh), want, "reference decoder vs generator");
        assert_eq!(real_chunked(&input, head_len, mb, mh), want, "ChunkState one-shot");
        // Growing prefixes through one state: Incomplete until the very end.
        let mut st = ChunkState::new(head_len);
        let mut body = Vec::new();
        let step = ((input.len() - head_len) / 300).max(1);
        let mut k = head_len;
        loop {
            let end = k.min(input.len());
            let r = st.decode(&input[..end], &mut body, mb, mh);
            if end < input.len() {
                assert_eq!(r, Ok(None), "chunked body complete early at {end}/{}", input.len());
            } else {
                assert_eq!(r, Ok(Some(input.len())));
                assert_eq!(body, payload);
                break;
            }
            k += step;
        }
    } else {
        assert_eq!(&input[head_len..], &payload[..]);
    }
}

fuzz_target!(|data: &[u8]| {
    match data.split_first() {
        Some((m, rest)) if m & 1 == 0 => structured(rest),
        Some((_, rest)) => raw_mode(rest),
        None => {}
    }
});
