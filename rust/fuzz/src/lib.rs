//! Shared helpers for the twine fuzz targets.
//!
//! Every byte-oriented target reads its input as
//! `[sel u8][max u16 LE][flags u8][nsplit u8][splits..][data..]`
//! (`nsplit % 16` split bytes, clamped to what is available); everything after
//! the header is the payload fed to the parser. This keeps corpus seeds
//! meaningful (header + raw wire bytes) while remaining a plain byte string for
//! libFuzzer's mutators.

/// Decoded fuzz input header plus payload.
pub struct Params<'a> {
    /// Free selector byte (delimiter / format / separator).
    pub sel: u8,
    /// Raw 16-bit limit field; targets use `max + 1` (1..=65536).
    pub max: u16,
    /// Free flag bits.
    pub flags: u8,
    /// Chunk-size bytes (each is a chunk length; 0 gives an empty chunk).
    pub splits: &'a [u8],
    /// The wire bytes.
    pub data: &'a [u8],
}

impl Params<'_> {
    /// Limit in `1..=65536`.
    pub fn limit(&self) -> usize {
        self.max as usize + 1
    }
}

/// Splits `input` into header and payload; `None` when too short.
pub fn params(input: &[u8]) -> Option<Params<'_>> {
    if input.len() < 5 {
        return None;
    }
    let n = (input[4] as usize % 16).min(input.len() - 5);
    Some(Params {
        sel: input[0],
        max: u16::from_le_bytes([input[1], input[2]]),
        flags: input[3],
        splits: &input[5..5 + n],
        data: &input[5 + n..],
    })
}

/// Cuts `data` into chunks whose sizes cycle through `splits`; the remainder
/// goes into a final chunk. Empty `splits` gives a single chunk.
pub fn chunk_list<'a>(data: &'a [u8], splits: &[u8]) -> Vec<&'a [u8]> {
    if splits.is_empty() {
        return vec![data];
    }
    let mut out = Vec::new();
    let mut pos = 0;
    let mut i = 0;
    let cap = data.len() + 2 * splits.len() + 8;
    while pos < data.len() && out.len() < cap {
        let end = (pos + splits[i % splits.len()] as usize).min(data.len());
        out.push(&data[pos..end]);
        pos = end;
        i += 1;
    }
    if pos < data.len() {
        out.push(&data[pos..]);
    }
    out
}

/// The documented framing errors, abstracted from `twine::Error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrKind {
    /// `Error::LineTooLong`.
    LineTooLong,
    /// `Error::FrameTooLarge`.
    TooLarge,
    /// `Error::Io` InvalidData as recognised by `twine_proto::is_malformed`.
    Malformed,
}

/// Classifies a parser error; panics on an undocumented error.
pub fn kind(e: &twine::Error) -> ErrKind {
    match e {
        twine::Error::LineTooLong => ErrKind::LineTooLong,
        twine::Error::FrameTooLarge => ErrKind::TooLarge,
        e if twine_proto::is_malformed(e) => ErrKind::Malformed,
        e => panic!("undocumented framing error: {e:?}"),
    }
}
