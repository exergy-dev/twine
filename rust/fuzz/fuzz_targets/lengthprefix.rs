#![no_main]
//! LengthPrefixParser over all 12 FrameFormat combinations.

use libfuzzer_sys::fuzz_target;
use twine_fuzz::{chunk_list, kind, params, ErrKind};
use twine_proto::{Endian, FrameFormat, Flow, LengthPrefixParser};

#[derive(Clone, Copy)]
struct Cfg {
    p: usize,
    little: bool,
    incl: bool,
    max: usize,
}

impl Cfg {
    fn fmt(&self) -> FrameFormat {
        FrameFormat::new()
            .with_prefix_len(self.p as u8)
            .with_endian(if self.little { Endian::Little } else { Endian::Big })
            .with_length_includes_prefix(self.incl)
            .with_max_frame(self.max)
    }
    /// Reference for `FrameFormat::max_sendable`.
    fn max_sendable(&self) -> usize {
        let mut lim: u64 = if self.p == 4 { u32::MAX as u64 } else { (1u64 << (8 * self.p)) - 1 };
        if self.incl {
            lim -= self.p as u64;
        }
        lim.min(self.max as u64) as usize
    }
    /// Reference prefix encoder (`None` when the body does not fit).
    fn encode(&self, body: usize) -> Option<Vec<u8>> {
        if body > self.max_sendable() {
            return None;
        }
        let v = (body + if self.incl { self.p } else { 0 }) as u64;
        let be = v.to_be_bytes();
        let mut b = be[8 - self.p..].to_vec();
        if self.little {
            b.reverse();
        }
        Some(b)
    }
}

struct Outcome {
    frames: Vec<Vec<u8>>,
    err: Option<ErrKind>,
    buffered: usize,
}

fn run_real(cfg: Cfg, chunks: &[&[u8]]) -> Outcome {
    let mut p = LengthPrefixParser::new(cfg.fmt());
    let mut frames = Vec::new();
    let mut err = None;
    for c in chunks {
        if let Err(e) = p.feed(c, &mut |f| {
            frames.push(f.to_vec());
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

/// Stream-level reference. Last value: bytes the parser should hold buffered
/// (partial header, or received body bytes once the header is complete).
fn ref_decode(data: &[u8], cfg: Cfg) -> (Vec<Vec<u8>>, Option<ErrKind>, usize) {
    let mut frames = Vec::new();
    let mut pos = 0;
    loop {
        let rest = &data[pos..];
        if rest.len() < cfg.p {
            return (frames, None, rest.len());
        }
        let mut hdr = rest[..cfg.p].to_vec();
        if cfg.little {
            hdr.reverse();
        }
        let v = hdr.iter().fold(0u64, |a, &b| (a << 8) | b as u64);
        let body = if cfg.incl {
            match v.checked_sub(cfg.p as u64) {
                Some(b) => b,
                None => return (frames, Some(ErrKind::Malformed), 0),
            }
        } else {
            v
        };
        if body > cfg.max as u64 {
            return (frames, Some(ErrKind::TooLarge), 0);
        }
        let body = body as usize;
        let after = &rest[cfg.p..];
        if after.len() < body {
            return (frames, None, after.len());
        }
        frames.push(after[..body].to_vec());
        pos += cfg.p + body;
    }
}

fuzz_target!(|input: &[u8]| {
    let Some(p) = params(input) else { return };
    let c = p.sel as usize % 12;
    let cfg = Cfg {
        p: [1, 2, 4][c % 3],
        little: (c / 3) % 2 == 1,
        incl: c / 6 == 1,
        max: if p.flags & 2 != 0 { usize::MAX } else { p.limit() },
    };
    let fmt = cfg.fmt();
    assert_eq!(fmt.max_sendable(), cfg.max_sendable());

    // 1. Arbitrary bytes against the reference, one-shot and chunked.
    let (want, want_err, want_buf) = ref_decode(p.data, cfg);
    for chunks in [vec![p.data], chunk_list(p.data, p.splits)] {
        let got = run_real(cfg, &chunks);
        assert_eq!(got.frames, want, "frames differ from reference");
        assert_eq!(got.err, want_err, "error differs from reference");
        if want_err.is_none() {
            assert_eq!(got.buffered, want_buf, "buffered bytes");
        }
    }

    // 2. Round trip of a frame list (data split on a selector byte).
    let sep = p.flags >> 2;
    let frames: Vec<Vec<u8>> = p.data.split(|b| *b == sep).map(|s| s.to_vec()).collect();
    let mut wire = Vec::new();
    let mut fitting = Vec::new();
    let mut first_big = None;
    for (i, f) in frames.iter().enumerate() {
        let enc = fmt.encode_prefix(f.len());
        assert_eq!(
            enc.as_ref().ok().map(|a| a[..cfg.p].to_vec()),
            cfg.encode(f.len()),
            "encode_prefix differs from reference"
        );
        match enc {
            Ok(pre) => {
                wire.extend_from_slice(&pre[..cfg.p]);
                wire.extend_from_slice(f);
                fitting.push(f.clone());
            }
            Err(e) => {
                assert_eq!(kind(&e), ErrKind::TooLarge);
                first_big.get_or_insert(i);
                break; // stop encoding at the first frame that cannot be sent
            }
        }
    }
    for chunks in [vec![&wire[..]], chunk_list(&wire, p.splits)] {
        let ok = run_real(cfg, &chunks);
        assert_eq!(ok.err, None, "valid encoding rejected");
        assert_eq!(ok.frames, fitting, "round trip");
        assert_eq!(ok.buffered, 0);

        // Tighter limit: the first frame above it is FrameTooLarge.
        if let Some(m) = fitting.iter().map(Vec::len).max().filter(|&m| m > 0) {
            let tight = Cfg { max: m - 1, ..cfg };
            let lim = run_real(tight, &chunks);
            let i = fitting.iter().position(|f| f.len() > m - 1).unwrap();
            assert_eq!(lim.err, Some(ErrKind::TooLarge));
            assert_eq!(lim.frames, fitting[..i]);
        }
    }
});
