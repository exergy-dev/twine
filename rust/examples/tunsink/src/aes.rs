//! AES-256-GCM frame decryption. Frame body = `nonce(12) || ciphertext || tag(16)`,
//! empty AAD; the plaintext is the IP packet.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};

/// Bytes added to every frame body: 12-byte nonce + 16-byte tag.
pub const OVERHEAD: usize = 28;

/// The fixed benchmark key shared with the Go side: byte i has value i.
pub fn default_key() -> [u8; 32] {
    std::array::from_fn(|i| i as u8)
}

/// Parses a 64-hex-character (32-byte) key.
pub fn parse_key(hex: &str) -> Result<[u8; 32], String> {
    let b = hex.as_bytes();
    if b.len() != 64 {
        return Err(format!("key must be 64 hex chars, got {}", b.len()));
    }
    let nib = |c: u8| -> Result<u8, String> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(format!("bad hex character {:?}", c as char)),
        }
    };
    let mut k = [0u8; 32];
    for (i, d) in k.iter_mut().enumerate() {
        *d = (nib(b[2 * i])? << 4) | nib(b[2 * i + 1])?;
    }
    Ok(k)
}

/// One AES-256-GCM key, shared by every connection via `Arc`.
pub struct Aes(LessSafeKey);

impl Aes {
    pub fn new(key: &[u8; 32]) -> Self {
        Aes(LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, key).expect("32-byte AES-256 key"),
        ))
    }

    /// Decrypts `body` into `dst` (cleared first; its capacity is reused).
    /// Returns false (and leaves `dst` empty) when the body is too short or the
    /// tag does not match.
    pub fn open_into(&self, body: &[u8], dst: &mut Vec<u8>) -> bool {
        dst.clear();
        if body.len() < OVERHEAD {
            return false;
        }
        let Ok(nonce) = Nonce::try_assume_unique_for_key(&body[..NONCE_LEN]) else {
            return false;
        };
        dst.extend_from_slice(&body[NONCE_LEN..]);
        let n = match self.0.open_in_place(nonce, Aad::empty(), dst) {
            Ok(p) => p.len(),
            Err(_) => {
                dst.clear(); // no unauthenticated bytes left behind
                return false;
            }
        };
        dst.truncate(n);
        true
    }

    /// Encrypts `plain` under `nonce` into a frame body `nonce || ct || tag`.
    #[cfg(test)]
    pub fn seal(&self, nonce: [u8; NONCE_LEN], plain: &[u8]) -> Vec<u8> {
        let mut ct = plain.to_vec();
        self.0
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut ct)
            .expect("seal");
        let mut body = Vec::with_capacity(NONCE_LEN + ct.len());
        body.extend_from_slice(&nonce);
        body.extend_from_slice(&ct);
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let a = Aes::new(&default_key());
        let body = a.seal([7; 12], b"hello, packet");
        assert_eq!(body.len(), 13 + OVERHEAD);
        let mut dst = Vec::new();
        assert!(a.open_into(&body, &mut dst));
        assert_eq!(dst, b"hello, packet");
        // Empty plaintext: exactly OVERHEAD bytes.
        let body = a.seal([1; 12], b"");
        assert_eq!(body.len(), OVERHEAD);
        assert!(a.open_into(&body, &mut dst));
        assert!(dst.is_empty());
    }

    #[test]
    fn wrong_key_and_tampering_fail() {
        let a = Aes::new(&default_key());
        let b = Aes::new(&[0xff; 32]);
        let mut body = b.seal([0; 12], b"secret");
        let mut dst = vec![1, 2, 3];
        assert!(!a.open_into(&body, &mut dst));
        assert!(dst.is_empty());
        assert!(b.open_into(&body, &mut dst));
        let last = body.len() - 1;
        body[last] ^= 1; // flipped tag byte
        assert!(!b.open_into(&body, &mut dst));
    }

    #[test]
    fn short_body_fails() {
        let a = Aes::new(&default_key());
        let mut dst = Vec::new();
        assert!(!a.open_into(&[0; OVERHEAD - 1], &mut dst));
        assert!(!a.open_into(&[], &mut dst));
    }

    #[test]
    fn keys() {
        let k = default_key();
        assert_eq!(k[0], 0);
        assert_eq!(k[31], 0x1f);
        let hex: String = (0..32u8).map(|i| format!("{i:02x}")).collect();
        assert_eq!(parse_key(&hex), Ok(k));
        assert_eq!(parse_key(&hex.to_uppercase()), Ok(k));
        assert_eq!(parse_key(&"ff".repeat(32)), Ok([0xff; 32]));
        assert!(parse_key(&hex[..63]).is_err());
        assert!(parse_key(&format!("{}zz", &hex[..62])).is_err());
        assert!(parse_key("").is_err());
    }
}
