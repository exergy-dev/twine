//! Property tests: encode random frames, split the stream into random chunks,
//! parse, compare with the originals. xorshift PRNG, fixed seeds.

mod common;

use common::Rng;
use twine::Error;
use twine_proto::{
    Endian, Flow, FrameFormat, Framed, LengthPrefixParser, LineParser, NetstringParser,
};

/// Splits `wire` into random chunk sizes in `1..=max_chunk`.
fn chunks<'a>(rng: &mut Rng, wire: &'a [u8], max_chunk: usize) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < wire.len() {
        let n = rng.range(1, max_chunk).min(wire.len() - i);
        out.push(&wire[i..i + n]);
        i += n;
    }
    out
}

fn random_bytes(rng: &mut Rng, alphabet: &[u8], len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| alphabet[rng.below(alphabet.len())])
        .collect()
}

fn line_prop(delim: &[u8], alphabet: &[u8], seed: u64) {
    let mut rng = Rng::new(seed);
    for round in 0..300 {
        let n = rng.range(0, 12);
        let mut lines: Vec<Vec<u8>> = Vec::new();
        while lines.len() < n {
            let len = if rng.below(8) == 0 {
                0
            } else {
                rng.range(1, 40)
            };
            let l = random_bytes(&mut rng, alphabet, len);
            if l.windows(delim.len()).any(|w| w == delim) {
                continue;
            }
            // A line must not combine with the delimiter into an earlier match.
            let mut probe = l.clone();
            probe.extend_from_slice(delim);
            if probe.windows(delim.len()).position(|w| w == delim) != Some(l.len()) {
                continue;
            }
            lines.push(l);
        }
        let mut wire = Vec::new();
        for l in &lines {
            wire.extend_from_slice(l);
            wire.extend_from_slice(delim);
        }
        let mut p = LineParser::new()
            .with_delimiter(delim)
            .with_max_line_length(64);
        let mut got = Vec::new();
        for c in chunks(&mut rng, &wire, 1 + round % 17) {
            let mut c = c.to_vec();
            p.feed(&mut c, &mut |f| {
                if let Framed::Line(l) = f {
                    got.push(l.to_vec());
                }
                Flow::Continue
            })
            .unwrap();
        }
        assert_eq!(got, lines, "round {round}");
        assert_eq!(p.buffered(), 0);
    }
}

#[test]
fn lines_default_delimiter_round_trip() {
    // '\r' in the alphabet exercises "\r\r\n" and CR at chunk edges.
    line_prop(b"\r\n", b"ab\r", 0x1234_5678);
}

#[test]
fn lines_lf_delimiter_round_trip() {
    line_prop(b"\n", b"abc\r ", 42);
}

#[test]
fn lines_multibyte_delimiter_round_trip() {
    line_prop(b"<>!", b"<>!x", 7);
    line_prop(b"aa", b"ab", 99);
}

#[test]
fn netstrings_round_trip() {
    let mut rng = Rng::new(0xdead_beef);
    for round in 0..300 {
        let n = rng.range(0, 10);
        let frames: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                let len = if rng.below(6) == 0 {
                    0
                } else {
                    rng.range(1, 300)
                };
                random_bytes(&mut rng, b"0123:,xyz\0", len)
            })
            .collect();
        let mut wire = Vec::new();
        for f in &frames {
            wire.extend_from_slice(format!("{}:", f.len()).as_bytes());
            wire.extend_from_slice(f);
            wire.push(b',');
        }
        let mut p = NetstringParser::new();
        let mut got = Vec::new();
        for c in chunks(&mut rng, &wire, 1 + round % 53) {
            p.feed(c, &mut |s| {
                got.push(s.to_vec());
                Flow::Continue
            })
            .unwrap();
        }
        assert_eq!(got, frames, "round {round}");
        assert_eq!(p.buffered(), 0);
    }
}

#[test]
fn length_prefixed_round_trip_all_formats() {
    let mut rng = Rng::new(0x0bad_cafe);
    for prefix in [1u8, 2, 4] {
        for endian in [Endian::Big, Endian::Little] {
            for inc in [false, true] {
                let fmt = FrameFormat::new()
                    .with_prefix_len(prefix)
                    .with_endian(endian)
                    .with_length_includes_prefix(inc);
                for round in 0..100 {
                    let n = rng.range(0, 10);
                    let frames: Vec<Vec<u8>> = (0..n)
                        .map(|_| {
                            let len = if rng.below(6) == 0 {
                                0
                            } else {
                                rng.range(1, 200)
                            };
                            random_bytes(&mut rng, &[0, 1, 2, 255, 7], len)
                        })
                        .collect();
                    let mut wire = Vec::new();
                    for f in &frames {
                        let pre = fmt.encode_prefix(f.len()).unwrap();
                        wire.extend_from_slice(&pre[..fmt.prefix_len()]);
                        wire.extend_from_slice(f);
                    }
                    let mut p = LengthPrefixParser::new(fmt);
                    let mut got = Vec::new();
                    for c in chunks(&mut rng, &wire, 1 + round % 41) {
                        p.feed(c, &mut |s| {
                            got.push(s.to_vec());
                            Flow::Continue
                        })
                        .unwrap();
                    }
                    assert_eq!(got, frames, "{fmt:?} round {round}");
                    assert_eq!(p.buffered(), 0);
                }
            }
        }
    }
}

/// Garbage never panics and never delivers more than it was fed; errors are the
/// documented ones.
#[test]
fn parsers_survive_random_garbage() {
    let mut rng = Rng::new(31337);
    for _ in 0..2000 {
        let len = rng.range(0, 200);
        let data = random_bytes(&mut rng, b"0123456789:,\r\nab\xff\0", len);
        let mut delivered = 0usize;

        let mut lp = LineParser::new().with_max_line_length(16);
        let mut d = data.clone();
        let r = lp.feed(&mut d, &mut |f| {
            if let Framed::Line(l) = f {
                delivered += l.len() + 2;
            }
            Flow::Continue
        });
        assert!(delivered <= data.len());
        assert!(matches!(r, Ok(()) | Err(Error::LineTooLong)));

        delivered = 0;
        let mut np = NetstringParser::new().with_max_length(32);
        let r = np.feed(&data, &mut |s| {
            delivered += s.len();
            Flow::Continue
        });
        assert!(delivered <= data.len());
        if let Err(e) = r {
            assert!(matches!(e, Error::FrameTooLarge) || twine_proto::is_malformed(&e));
        }

        delivered = 0;
        let fmt = FrameFormat::new().with_prefix_len(2).with_max_frame(64);
        let mut fp = LengthPrefixParser::new(fmt);
        let r = fp.feed(&data, &mut |s| {
            delivered += s.len();
            Flow::Continue
        });
        assert!(delivered <= data.len());
        assert!(matches!(r, Ok(()) | Err(Error::FrameTooLarge)));
    }
}
