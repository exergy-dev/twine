#![no_main]
//! NetstringParser: reference decoder, round trip under chunking, documented
//! errors only.

use libfuzzer_sys::fuzz_target;
use twine_fuzz::{chunk_list, kind, params, ErrKind};
use twine_proto::{Flow, NetstringParser};

struct Outcome {
    frames: Vec<Vec<u8>>,
    err: Option<ErrKind>,
    buffered: usize,
}

fn run_real(max: usize, chunks: &[&[u8]]) -> Outcome {
    let mut p = NetstringParser::new().with_max_length(max);
    let mut frames = Vec::new();
    let mut err = None;
    for c in chunks {
        if let Err(e) = p.feed(c, &mut |s| {
            frames.push(s.to_vec());
            Flow::Continue
        }) {
            err = Some(kind(&e));
            break;
        }
    }
    if err.is_some() {
        assert!(p.is_stopped());
    }
    Outcome {
        frames,
        err,
        buffered: p.buffered(),
    }
}

/// Stream-level reference decoder. Returns frames, error and the number of
/// payload bytes of the incomplete trailing frame already received.
fn ref_decode(data: &[u8], max: usize) -> (Vec<Vec<u8>>, Option<ErrKind>, usize) {
    let mut frames = Vec::new();
    let mut pos = 0;
    loop {
        let mut n = 0usize;
        let mut digits = 0;
        loop {
            let Some(&c) = data.get(pos) else {
                return (frames, None, 0);
            };
            pos += 1;
            match c {
                b'0'..=b'9' => {
                    if digits > 0 && n == 0 {
                        return (frames, Some(ErrKind::Malformed), 0);
                    }
                    n = n * 10 + (c - b'0') as usize;
                    digits += 1;
                    if n > max {
                        return (frames, Some(ErrKind::TooLarge), 0);
                    }
                }
                b':' if digits > 0 => break,
                _ => return (frames, Some(ErrKind::Malformed), 0),
            }
        }
        let avail = data.len() - pos;
        if avail < n {
            return (frames, None, avail);
        }
        let payload = &data[pos..pos + n];
        pos += n;
        match data.get(pos) {
            None => return (frames, None, n),
            Some(b',') => {
                frames.push(payload.to_vec());
                pos += 1;
            }
            Some(_) => return (frames, Some(ErrKind::Malformed), 0),
        }
    }
}

fn encode(payloads: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in payloads {
        out.extend_from_slice(p.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(p);
        out.push(b',');
    }
    out
}

fuzz_target!(|input: &[u8]| {
    let Some(p) = params(input) else { return };
    let max = p.limit();

    // 1. Arbitrary bytes against the reference, one-shot and chunked.
    let (want, want_err, want_buf) = ref_decode(p.data, max);
    for chunks in [vec![p.data], chunk_list(p.data, p.splits)] {
        let got = run_real(max, &chunks);
        assert_eq!(got.frames, want, "frames differ from reference");
        assert_eq!(got.err, want_err, "error differs from reference");
        if want_err.is_none() {
            assert_eq!(got.buffered, want_buf, "buffered payload bytes");
        }
    }

    // 2. Round trip: payloads are the data split on the selector byte.
    let payloads: Vec<Vec<u8>> = p.data.split(|b| *b == p.sel).map(|s| s.to_vec()).collect();
    let wire = encode(&payloads);
    let longest = payloads.iter().map(Vec::len).max().unwrap_or(0);
    let chunks = chunk_list(&wire, p.splits);
    for chunks in [vec![&wire[..]], chunks] {
        let ok = run_real(max.max(longest), &chunks);
        assert_eq!(ok.err, None, "valid encoding rejected");
        assert_eq!(ok.frames, payloads, "round trip");
        assert_eq!(ok.buffered, 0);

        // With the limit `max`, the first oversize payload is FrameTooLarge
        // and everything before it is delivered.
        let lim = run_real(max, &chunks);
        match payloads.iter().position(|x| x.len() > max) {
            None => {
                assert_eq!(lim.err, None);
                assert_eq!(lim.frames, payloads);
            }
            Some(i) => {
                assert_eq!(lim.err, Some(ErrKind::TooLarge));
                assert_eq!(lim.frames, payloads[..i]);
            }
        }
    }
});
