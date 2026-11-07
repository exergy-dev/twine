#![no_main]
//! LineParser: differential check against a naive reference splitter, under
//! one-shot and arbitrary chunked feeding, with optional raw-mode toggles.

use libfuzzer_sys::fuzz_target;
use twine_fuzz::{chunk_list, kind, params, ErrKind};
use twine_proto::{Flow, Framed, LineParser};

#[derive(Debug, PartialEq, Eq, Clone)]
enum Ev {
    Line(Vec<u8>),
    Raw(Vec<u8>),
}

/// Callback policy derived from the flag byte.
#[derive(Clone, Copy)]
struct Pol {
    /// Return `RawMode` from the line with this 0-based index.
    raw_after: Option<usize>,
    /// Raw chunks return `LineMode(k)` (clamped to the chunk length).
    lm: Option<usize>,
}

impl Pol {
    fn from_flags(f: u8) -> Pol {
        const KS: [usize; 8] = [0, 1, 2, 3, 5, 8, 13, 1000];
        Pol {
            raw_after: (f & 1 != 0).then(|| ((f >> 1) & 7) as usize),
            lm: (f & 0x10 != 0).then(|| KS[(f >> 5) as usize]),
        }
    }
}

struct Outcome {
    evs: Vec<Ev>,
    err: Option<ErrKind>,
    buffered: usize,
}

fn run_real(d: &[u8], max: usize, pol: Pol, chunks: &[&[u8]]) -> Outcome {
    let mut p = LineParser::new().with_delimiter(d).with_max_line_length(max);
    let mut evs = Vec::new();
    let mut n_lines = 0usize;
    let mut err = None;
    for c in chunks {
        let mut buf = c.to_vec();
        let r = p.feed(&mut buf, &mut |fr| match fr {
            Framed::Line(l) => {
                evs.push(Ev::Line(l.to_vec()));
                let idx = n_lines;
                n_lines += 1;
                if pol.raw_after == Some(idx) {
                    Flow::RawMode
                } else {
                    Flow::Continue
                }
            }
            Framed::Raw(r) => {
                evs.push(Ev::Raw(r.to_vec()));
                match pol.lm {
                    Some(k) => Flow::LineMode(k),
                    None => Flow::Continue,
                }
            }
        });
        if let Err(e) = r {
            err = Some(kind(&e));
            break;
        }
    }
    // `buffered() <= max + delimiter - 1` (a partial line may end in a
    // delimiter prefix); Go's fuzzer asserted the same bound.
    assert!(p.buffered() <= max + d.len() - 1, "buffered over bound");
    if err.is_some() {
        assert!(p.is_stopped());
        assert_eq!(p.buffered(), 0, "buffer not released on error");
    }
    Outcome {
        evs,
        err,
        buffered: p.buffered(),
    }
}

fn naive_find(hay: &[u8], d: &[u8]) -> Option<usize> {
    if hay.len() < d.len() {
        return None;
    }
    (0..=hay.len() - d.len()).find(|&i| hay[i..].starts_with(d))
}

/// Naive reference: keeps the whole partial line, rescans it from the start.
struct Ref<'a> {
    d: &'a [u8],
    max: usize,
    pol: Pol,
    pending: Vec<u8>,
    raw: bool,
    n_lines: usize,
    evs: Vec<Ev>,
    err: bool,
}

impl Ref<'_> {
    fn feed(&mut self, chunk: &[u8]) {
        if self.err {
            return;
        }
        let mut input = chunk.to_vec();
        loop {
            if input.is_empty() {
                return;
            }
            if self.raw {
                self.evs.push(Ev::Raw(input.clone()));
                match self.pol.lm {
                    Some(k) => {
                        let k = k.min(input.len());
                        self.raw = false;
                        input = input[input.len() - k..].to_vec();
                    }
                    None => return,
                }
                continue;
            }
            self.pending.extend_from_slice(&input);
            input.clear();
            loop {
                match naive_find(&self.pending, self.d) {
                    Some(i) => {
                        if i > self.max {
                            self.err = true;
                            return;
                        }
                        self.evs.push(Ev::Line(self.pending[..i].to_vec()));
                        self.pending.drain(..i + self.d.len());
                        let idx = self.n_lines;
                        self.n_lines += 1;
                        if self.pol.raw_after == Some(idx) {
                            self.raw = true;
                            input = std::mem::take(&mut self.pending);
                            break;
                        }
                    }
                    None => {
                        if self.pending.len() > self.max + self.d.len() - 1 {
                            self.err = true;
                        }
                        return;
                    }
                }
            }
        }
    }
}

fn run_ref(d: &[u8], max: usize, pol: Pol, chunks: &[&[u8]]) -> (Vec<Ev>, bool, usize) {
    let mut r = Ref {
        d,
        max,
        pol,
        pending: Vec::new(),
        raw: false,
        n_lines: 0,
        evs: Vec::new(),
        err: false,
    };
    for c in chunks {
        r.feed(c);
    }
    let buffered = if r.err || r.raw { 0 } else { r.pending.len() };
    (r.evs, r.err, buffered)
}

/// Merges adjacent raw events (chunking changes their boundaries).
fn normalize(evs: &[Ev]) -> Vec<Ev> {
    let mut out: Vec<Ev> = Vec::new();
    for e in evs {
        match (out.last_mut(), e) {
            (Some(Ev::Raw(a)), Ev::Raw(b)) => a.extend_from_slice(b),
            _ => out.push(e.clone()),
        }
    }
    out
}

fn check(d: &[u8], max: usize, pol: Pol, data: &[u8], chunks: &[&[u8]]) -> Outcome {
    let got = run_real(d, max, pol, chunks);
    let (want, want_err, want_buf) = run_ref(d, max, pol, chunks);
    assert_eq!(got.evs, want, "events differ from reference");
    assert_eq!(
        got.err,
        want_err.then_some(ErrKind::LineTooLong),
        "LineTooLong mismatch"
    );
    if !want_err {
        assert_eq!(got.buffered, want_buf, "buffered tail length");
    }
    // Reconstruction: delivered lines + delimiters + raw bytes + buffered tail
    // is exactly the input (only when no raw chunk is handed back, which
    // delivers some bytes twice).
    if got.err.is_none() && pol.lm.is_none() {
        let mut flat = Vec::new();
        for e in &got.evs {
            match e {
                Ev::Line(l) => {
                    flat.extend_from_slice(l);
                    flat.extend_from_slice(d);
                }
                Ev::Raw(r) => flat.extend_from_slice(r),
            }
        }
        assert_eq!(flat.len() + got.buffered, data.len(), "reconstruction length");
        assert_eq!(&flat[..], &data[..flat.len()], "reconstruction bytes");
    }
    got
}

fuzz_target!(|input: &[u8]| {
    let Some(p) = params(input) else { return };
    let d: &[u8] = match p.sel % 3 {
        0 => b"\r\n",
        1 => b"\n",
        _ => b"||",
    };
    let max = p.limit();
    let pol = Pol::from_flags(p.flags);

    let whole = check(d, max, pol, p.data, &[p.data]);
    let chunks = chunk_list(p.data, p.splits);
    let chunked = check(d, max, pol, p.data, &chunks);

    // Chunk invariance (without LineMode, whose meaning depends on chunk
    // boundaries): same lines, same raw bytes, same verdict.
    if pol.lm.is_none() {
        assert_eq!(whole.err, chunked.err, "error differs with chunking");
        assert_eq!(
            normalize(&whole.evs),
            normalize(&chunked.evs),
            "events differ with chunking"
        );
    }
});
