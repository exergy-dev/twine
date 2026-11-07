//! Parser limits at the extremes (`usize::MAX`, 0) and buffer retention.

use twine::Error;
use twine_proto::{Flow, Framed, LineParser, NetstringParser};

fn lines(p: &mut LineParser, chunks: &[&[u8]]) -> Result<Vec<Vec<u8>>, Error> {
    let mut out = Vec::new();
    for c in chunks {
        let mut d = c.to_vec();
        p.feed(&mut d, &mut |fr| {
            if let Framed::Line(l) = fr {
                out.push(l.to_vec());
            }
            Flow::Continue
        })?;
    }
    Ok(out)
}

#[test]
fn line_max_usize_max_does_not_overflow() {
    for d in [&b"\r\n"[..], b"\n", b"<END>"] {
        let mut p = LineParser::new()
            .with_delimiter(d)
            .with_max_line_length(usize::MAX);
        // A long line spanning many chunks, then another line.
        let big = vec![b'a'; 100_000];
        let mut tail = b"xy".to_vec();
        tail.extend_from_slice(d);
        let got = lines(&mut p, &[&big, &big, &tail, b"next", d]).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].len(), 200_002);
        assert_eq!(got[1], b"next");
        // No delimiter at all: buffered, no panic, no error.
        let got = lines(&mut p, &[&big, &big]).unwrap();
        assert!(got.is_empty());
        assert_eq!(p.buffered(), 200_000);
    }
}

#[test]
fn line_limit_zero_means_only_empty_lines() {
    let mut p = LineParser::new().with_max_line_length(0);
    assert_eq!(
        lines(&mut p, &[b"\r\n\r\n"]).unwrap(),
        vec![b"".to_vec(); 2]
    );
    let mut p = LineParser::new().with_max_line_length(0);
    assert!(matches!(
        lines(&mut p, &[b"a\r\n"]),
        Err(Error::LineTooLong)
    ));
}

#[test]
fn line_limit_exact_boundary() {
    // max 4, delimiter 2: "abcd\r\n" ok, "abcde\r\n" too long, however split.
    for split in 0..=7 {
        let ok = b"abcd\r\n";
        let (a, b) = ok.split_at(split.min(ok.len()));
        let mut p = LineParser::new().with_max_line_length(4);
        assert_eq!(lines(&mut p, &[a, b]).unwrap(), vec![b"abcd".to_vec()]);
        let bad = b"abcde\r\n";
        let (a, b) = bad.split_at(split.min(bad.len()));
        let mut p = LineParser::new().with_max_line_length(4);
        assert!(matches!(lines(&mut p, &[a, b]), Err(Error::LineTooLong)));
    }
}

#[test]
fn large_lines_spanning_chunks_keep_their_buffer() {
    // 10 KiB lines (above the old fixed 4 KiB retention, below the 16 KiB
    // limit) delivered in two chunks each: the buffer is reused.
    let mut p = LineParser::new();
    let line = vec![b'z'; 10 << 10];
    let mut count = 0;
    let mut cap = 0;
    for i in 0..5 {
        for part in [&line[..], b"\r\n"] {
            let mut d = part.to_vec();
            p.feed(&mut d, &mut |fr| {
                if let Framed::Line(l) = fr {
                    assert_eq!(l.len(), line.len());
                    count += 1;
                }
                Flow::Continue
            })
            .unwrap();
        }
        if i == 0 {
            cap = p.buffer_capacity();
            assert!(cap >= line.len());
        } else {
            assert_eq!(p.buffer_capacity(), cap, "buffer reallocated on line {i}");
        }
    }
    assert_eq!(count, 5);
}

fn ns(p: &mut NetstringParser, chunks: &[&[u8]]) -> Result<Vec<Vec<u8>>, Error> {
    let mut out = Vec::new();
    for c in chunks {
        p.feed(c, &mut |s| {
            out.push(s.to_vec());
            Flow::Continue
        })?;
    }
    Ok(out)
}

#[test]
fn netstring_length_overflow_is_frame_too_large() {
    let mut p = NetstringParser::new().with_max_length(usize::MAX);
    // 40 digits would wrap a usize many times over.
    let digits = vec![b'9'; 40];
    assert!(matches!(ns(&mut p, &[&digits]), Err(Error::FrameTooLarge)));
    // usize::MAX itself is accepted as a length (the payload never completes).
    let mut p = NetstringParser::new().with_max_length(usize::MAX);
    let s = format!("{}:", usize::MAX);
    assert!(ns(&mut p, &[s.as_bytes()]).unwrap().is_empty());
    // One more than usize::MAX overflows.
    let mut p = NetstringParser::new().with_max_length(usize::MAX);
    let s = format!("{}0:", usize::MAX);
    assert!(matches!(
        ns(&mut p, &[s.as_bytes()]),
        Err(Error::FrameTooLarge)
    ));
    // Normal operation under usize::MAX.
    let mut p = NetstringParser::new().with_max_length(usize::MAX);
    assert_eq!(
        ns(&mut p, &[b"5:hello,0:,"]).unwrap(),
        vec![b"hello".to_vec(), b"".to_vec()]
    );
}

#[test]
fn netstring_limit_zero_means_only_empty() {
    let mut p = NetstringParser::new().with_max_length(0);
    assert_eq!(ns(&mut p, &[b"0:,0:,"]).unwrap(), vec![b"".to_vec(); 2]);
    let mut p = NetstringParser::new().with_max_length(0);
    assert!(matches!(ns(&mut p, &[b"1:a,"]), Err(Error::FrameTooLarge)));
}
