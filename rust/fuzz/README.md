# twine fuzz targets

`cargo fuzz` project (standalone, not in the root workspace). Requires nightly
and `cargo install cargo-fuzz`. Run from `rust/`:

    export CARGO_TARGET_DIR=$PWD/fuzz/target     # keep build output under fuzz/
    cargo +nightly fuzz run line         -- -max_total_time=60 -timeout=10
    cargo +nightly fuzz run netstring    -- -max_total_time=60 -timeout=10
    cargo +nightly fuzz run lengthprefix -- -max_total_time=60 -timeout=10
    cargo +nightly fuzz run http_parse   -- -max_total_time=60 -timeout=10

All targets in parallel with a summary table (default 600 s, non-zero exit on a crash):

    scripts/fuzz.sh [seconds]

Replay a finding: `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.

## Input format

Byte targets read `[sel u8][max u16 LE][flags u8][nsplit u8][splits..][data..]`
(see `src/lib.rs`): `sel` picks delimiter / frame format / separator, `max+1` is
the size limit, `splits` are chunk sizes cycled over the data. `http_parse`
prefixes one more byte: even = structured request from `arbitrary`, odd = raw
wire bytes behind the common header.

## Targets

- `line`: `LineParser` vs a naive splitter, one-shot and chunked, raw-mode and
  `LineMode` toggles; `LineTooLong` iff the reference finds an over-long line;
  `buffered() <= max + delimiter.len() - 1`.
- `netstring`: reference decoder; valid encodings round-trip under any chunking;
  limit violations are `FrameTooLarge`; other errors are `is_malformed`.
- `lengthprefix`: all 12 `FrameFormat` prefix/endian/includes-prefix combos;
  reference decoder, round trip, `encode_prefix` vs reference, tighter limit.
- `http_parse`: `parse_request` never panics, offsets stay inside the head, every
  proper prefix is `Incomplete`; structured requests round-trip all fields;
  `ChunkState` (one-shot and incremental) vs a reference chunked decoder.

## Corpus

`corpus/<target>/seed_*.bin` are committed (Go fuzz seeds converted plus hand
written cases); regenerate with `python3 -I fuzz/gen_seeds.py`. Other corpus
files and `artifacts/` are git-ignored.

`artifacts/http_parse/trailer-limit-split-crlf` is a known finding (ChunkState
returns 431 for a valid body when the trailer bytes exactly fill `max_hdr` and
the final CRLF is split between `\r` and `\n`); it makes `http_parse` fail until
the parser is fixed.
