#!/usr/bin/env python3
"""Regenerates fuzz/corpus/<target>/seed_*.bin from the Go fuzz corpora plus
hand-written cases.  Run: python3 -I fuzz/gen_seeds.py  (from rust/)."""
import os, re, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
GO = os.path.join(HERE, "..", "..")
OUT = os.path.join(HERE, "corpus")
ESC = {'a': 7, 'b': 8, 'f': 12, 'n': 10, 'r': 13, 't': 9, 'v': 11, '\\': 92, '"': 34, "'": 39}


def unquote(s):
    out = bytearray()
    i = 0
    while i < len(s):
        c = s[i]
        if c != '\\':
            out += c.encode('utf-8'); i += 1; continue
        e = s[i + 1]
        if e in ESC:
            out.append(ESC[e]); i += 2
        elif e == 'x':
            out.append(int(s[i + 2:i + 4], 16)); i += 4
        elif e in 'uU':
            n = 4 if e == 'u' else 8
            out += chr(int(s[i + 2:i + 2 + n], 16)).encode('utf-8'); i += 2 + n
        elif e in '01234567':
            out.append(int(s[i + 1:i + 4], 8)); i += 4
        else:
            raise ValueError(s)
    return bytes(out)


def parse_go(path):
    vals = []
    with open(path, encoding='utf-8') as f:
        lines = f.read().split('\n')
    assert lines[0] == 'go test fuzz v1'
    for ln in lines[1:]:
        if not ln.strip():
            continue
        m = re.fullmatch(r'(\[\]byte|byte|uint8|uint16|int|string)\((.*)\)', ln)
        t, a = m.group(1), m.group(2)
        if t in ('[]byte', 'string'):
            vals.append(unquote(a[1:-1]))
        elif a.startswith("'"):
            vals.append(unquote(a[1:-1])[0])
        else:
            vals.append(int(a, 0))
    return vals


def hdr(sel, maxfield, flags, splits, data):
    splits = bytes(min(max(b, 0), 255) for b in splits[:15])
    return bytes([sel & 255]) + struct.pack('<H', maxfield & 0xFFFF) + bytes([flags & 255, len(splits)]) + splits + data


def go_splits(b):  # Go chunk sizes are byte+1
    return bytes(min(x + 1, 255) for x in b)


def write(target, name, blob, seen):
    if blob in seen:
        return
    seen.add(blob)
    d = os.path.join(OUT, target)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name), 'wb') as f:
        f.write(blob)


def go_corpus(target, gosub, conv):
    base = os.path.join(GO, gosub)
    seen = set()
    if not os.path.isdir(base):
        return seen
    for fn in sorted(os.listdir(base)):
        v = parse_go(os.path.join(base, fn))
        write(target, 'seed_go_' + fn + '.bin', conv(v), seen)
    return seen


DELIM = {0: 0, 1: 1, 4: 2}


def line_conv(v):
    data, dsel, maxlen, split = v
    return hdr(DELIM.get(dsel % 5, 0), maxlen % 1024, 0, go_splits(split), data)


def ns_conv(v):
    data, maxlen, split = v
    return hdr(0x2C, maxlen % 4096, 0, go_splits(split), data)


def lp_conv(v):
    data, prefix, flags, maxf, split = v
    c = prefix % 3 + 3 * (flags & 1) + 6 * ((flags >> 1) & 1)
    return hdr(c, maxf, 0, go_splits(split), data)


def http_conv(v):
    data, split = v
    return b'\x01' + hdr(0, 0, 0, bytes([split & 255]) if split else b'', data)


def main():
    s = go_corpus('line', 'proto/testdata/fuzz/FuzzLineReceiver', line_conv)
    # hand-written
    for i, (sel, mx, fl, sp, d) in enumerate([
        (0, 1023, 0, b'\x01', b'hello\r\nworld\r\n'),
        (1, 1023, 0, b'\x03\x02', b'a\nb\n\nc'),
        (2, 1023, 0, b'\x05', b'x||y||||z|'),
        (0, 4, 0, b'', b'12345\r\n'),                 # LineTooLong
        (0, 4, 0, b'\x01', b'12345\r'),               # partial at the eager bound
        (0, 1023, 0x01, b'\x04', b'a\r\nb\r\nraw\r\nbytes'),   # RawMode after line 0
        (0, 1023, 0x03 | 0x10 | 0x40, b'\x07', b'a\r\nb\r\nc\r\nd\r\n'),  # raw + LineMode(5)
        (1, 1023, 0x10 | 0x20, b'\x03', b'1\n2\n3\n'),
        (0, 1023, 0, b'\x01', b''),
    ]):
        write('line', 'seed_hand_%02d.bin' % i, hdr(sel, mx, fl, sp, d), s)

    s = go_corpus('netstring', 'proto/testdata/fuzz/FuzzNetstring', ns_conv)
    for i, (mx, sp, d) in enumerate([
        (4095, b'\x01', b'5:hello,0:,'),
        (4095, b'\x03\x02', b'3:abc,3:de'),
        (4095, b'', b'01:a,'),                        # leading zero
        (4095, b'\x02', b'10:0123456789,'),
        (4095, b'', b'2:ab;'),                        # bad terminator
        (4095, b'', b':x,'),
        (1000, b'\x07', b'999999999999999999999999:'),
        (3, b'\x01', b'5:hello,'),                    # FrameTooLarge
        (4095, b'\x01', b'0:,0:,0:,'),
        (4095, b'\x01\x00\x02', b'1:a,2:bc,3:def,4:ghij,'),
    ]):
        write('netstring', 'seed_hand_%02d.bin' % i, hdr(0x2C, mx, 0, sp, d), s)

    s = go_corpus('lengthprefix', 'proto/testdata/fuzz/FuzzLengthPrefixed', lp_conv)
    for i, (sel, mx, fl, sp, d) in enumerate([
        (2, 99, 0, b'\x01', b'\0\0\0\3abc\0\0\0\0'),            # 4B BE
        (0, 99, 0, b'', b'\3abc\0'),                              # 1B
        (1, 99, 0, b'\x01\x02', b'\0\5abc'),                       # truncated
        (3 + 1, 99, 0, b'\x02', b'\3\0x\0\0'),                    # 2B LE
        (2 + 6, 99, 0, b'\x01', b'\0\0\0\4'),                      # includes prefix, empty body
        (2 + 6, 99, 0, b'', b'\0\0\0\3'),                          # malformed: < prefix
        (2, 9, 0, b'', b'\0\0\1\0'),                               # FrameTooLarge
        (9, 99, 0x02, b'\x03', b'\5\0\0\0hello\0\0\0\0'),          # 4B LE, max=usize::MAX
        (6, 99, 0, b'\x01', b'\0\2ab\0\4abcd'),                    # 1B incl
    ]):
        write('lengthprefix', 'seed_hand_%02d.bin' % i, hdr(sel, mx, fl, sp, d), s)

    s = go_corpus('http_parse', 'http/testdata/fuzz/FuzzParseRequest', http_conv)
    reqs = [
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
        b"GET /a/b%20c?x=1&y=%41 HTTP/1.1\r\nHost: example.com:8080\r\nUser-Agent: test\r\nAccept: */*\r\n\r\n",
        b"POST /submit HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"a\":\"hello\"}",
        b"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6;name=v\r\n world\r\n0\r\n\r\n",
        b"GET /old HTTP/1.0\r\n\r\n",
        b"OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n",
        b"GET http://example.com/abs?q=1 HTTP/1.1\r\nHost: ignored\r\n\r\n",
        b"DELETE /x HTTP/1.1\r\nHost: x\r\nX-Multi: a\r\nx-multi: b\r\nX-Empty:\r\nX-Spaces: \t v \t\r\n\r\n",
        b"PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc",
        b"GET/ HTTP/1.1\r\nHost: x\r\n\r\n",                                   # bad request line
        b"GET / HTTP/2.0\r\nHost: x\r\n\r\n",                                   # 505
        b"GET / HTTP/1.1\r\n\r\n",                                              # missing Host
        b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi",
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nX-T: v\r\n\r\n",
        b"3\r\nabc\r\n0\r\n\r\n",                                               # bare chunked body (ChunkState from 0)
    ]
    for i, r in enumerate(reqs):
        write('http_parse', 'seed_hand_%02d.bin' % i, b'\x01' + hdr(0, 0, 0, b'\x0d\x07', r), s)
        write('http_parse', 'seed_hand_small_%02d.bin' % i, b'\x01' + hdr(0, 0x20, 1, b'\x01', r), s)
    # structured-mode seeds (first byte even): arbitrary bytes drive the generator
    import random
    rnd = random.Random(1234)
    for i in range(8):
        write('http_parse', 'seed_struct_%02d.bin' % i, b'\x00' + bytes(rnd.randrange(256) for _ in range(160 + 40 * i)), s)

    for t in os.listdir(OUT):
        print(t, len(os.listdir(os.path.join(OUT, t))))


main()
