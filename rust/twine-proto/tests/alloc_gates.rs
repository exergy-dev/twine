//! Allocation gates: the zero-copy path allocates nothing, and buffering a
//! partial frame allocates only while the buffer grows (steady state 0).

use twine_alloccount::allocs;
use twine_proto::{
    Endian, Flow, FrameFormat, Framed, LengthPrefixParser, LineParser, NetstringParser,
};

#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

fn report(name: &str, n: u64) {
    eprintln!("GATE {name}: allocs = {n}");
}

#[test]
fn gate_lines_complete_chunk_is_zero_copy() {
    let mut p = LineParser::new();
    let mut data = b"alpha\r\nbeta\r\n\r\ngamma delta\r\n".to_vec();
    let mut total = 0usize;
    let n = allocs(|| {
        for _ in 0..1000 {
            p.feed(&mut data, &mut |f| {
                if let Framed::Line(l) = f {
                    total += l.len();
                }
                Flow::Continue
            })
            .unwrap();
        }
    });
    report("lines complete chunk x1000", n);
    assert_eq!(total, 1000 * (5 + 4 + 11));
    assert_eq!(n, 0);
}

#[test]
fn gate_lines_partial_steady_state() {
    let mut p = LineParser::new();
    let mut a = b"par".to_vec();
    let mut b = b"tial\r\nfull\r\nnext".to_vec();
    let mut c = b"\r\n".to_vec();
    let mut cnt = 0usize;
    let mut step = |p: &mut LineParser| {
        for d in [&mut a, &mut b, &mut c] {
            p.feed(d, &mut |_| {
                cnt += 1;
                Flow::Continue
            })
            .unwrap();
        }
    };
    let warm = allocs(|| step(&mut p));
    let steady = allocs(|| {
        for _ in 0..1000 {
            step(&mut p);
        }
    });
    report("lines partial warm-up", warm);
    report("lines partial steady x1000", steady);
    assert!(warm >= 1, "first partial must allocate");
    assert_eq!(steady, 0);
    assert_eq!(cnt, 3 * 1001);
}

#[test]
fn gate_lines_delimiter_split_and_custom() {
    let mut p = LineParser::new().with_delimiter(b"<>!");
    let mut a = b"x<>".to_vec();
    let mut b = b"!y<>!".to_vec();
    let mut step = |p: &mut LineParser| {
        for d in [&mut a, &mut b] {
            p.feed(d, &mut |_| Flow::Continue).unwrap();
        }
    };
    let _ = allocs(|| step(&mut p));
    let steady = allocs(|| {
        for _ in 0..1000 {
            step(&mut p);
        }
    });
    report("lines custom delimiter split steady x1000", steady);
    assert_eq!(steady, 0);
}

#[test]
fn gate_netstring() {
    let mut p = NetstringParser::new();
    let data = b"5:hello,0:,11:hello world,";
    let mut total = 0usize;
    let n = allocs(|| {
        for _ in 0..1000 {
            p.feed(data, &mut |s| {
                total += s.len();
                Flow::Continue
            })
            .unwrap();
        }
    });
    report("netstring complete chunk x1000", n);
    assert_eq!(total, 16_000);
    assert_eq!(n, 0);

    let (a, b, c): (&[u8], &[u8], &[u8]) = (b"11:hello", b" wor", b"ld,3:abc,");
    let step = |p: &mut NetstringParser| {
        for d in [a, b, c] {
            p.feed(d, &mut |_| Flow::Continue).unwrap();
        }
    };
    let warm = allocs(|| step(&mut p));
    let steady = allocs(|| {
        for _ in 0..1000 {
            step(&mut p);
        }
    });
    report("netstring partial warm-up", warm);
    report("netstring partial steady x1000", steady);
    assert!(warm >= 1);
    assert_eq!(steady, 0);
}

#[test]
fn gate_length_prefixed() {
    for fmt in [
        FrameFormat::new(),
        FrameFormat::new()
            .with_prefix_len(2)
            .with_endian(Endian::Little),
        FrameFormat::new()
            .with_prefix_len(1)
            .with_length_includes_prefix(true),
    ] {
        let mut wire = Vec::new();
        for body in [&b"hello"[..], b"", b"world!!"] {
            let pre = fmt.encode_prefix(body.len()).unwrap();
            wire.extend_from_slice(&pre[..fmt.prefix_len()]);
            wire.extend_from_slice(body);
        }
        let mut p = LengthPrefixParser::new(fmt);
        let mut total = 0usize;
        let n = allocs(|| {
            for _ in 0..1000 {
                p.feed(&wire, &mut |s| {
                    total += s.len();
                    Flow::Continue
                })
                .unwrap();
            }
        });
        report(
            &format!("length-prefixed complete chunk x1000 ({fmt:?})"),
            n,
        );
        assert_eq!(total, 12_000);
        assert_eq!(n, 0);

        // Split mid-header and mid-body.
        let cut1 = fmt.prefix_len().div_ceil(2).max(1).min(wire.len() - 1);
        let cut2 = wire.len() - 3;
        let parts = [&wire[..cut1], &wire[cut1..cut2], &wire[cut2..]];
        let step = |p: &mut LengthPrefixParser| {
            for d in parts {
                p.feed(d, &mut |_| Flow::Continue).unwrap();
            }
        };
        let warm = allocs(|| step(&mut p));
        let steady = allocs(|| {
            for _ in 0..1000 {
                step(&mut p);
            }
        });
        report(&format!("length-prefixed partial warm-up ({fmt:?})"), warm);
        report(
            &format!("length-prefixed partial steady x1000 ({fmt:?})"),
            steady,
        );
        assert_eq!(steady, 0);
    }
}
