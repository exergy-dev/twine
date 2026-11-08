//! Standalone cost of one AES-256-GCM open of a SIZE-byte frame
//! (nonce || ciphertext || tag) into a preallocated buffer, single thread: the
//! per-frame decrypt cost the `-aes` server pays (copy into the buffer included,
//! as in the server).
//!
//! `taskset -c 2 cargo run --release --bin aesbench -- 512`
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let size: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(512);
    let n: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000);
    let key: [u8; 32] = std::array::from_fn(|i| i as u8);
    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &key).unwrap());
    let nonce = [0u8; 12];
    let mut ct = vec![0u8; size];
    key.seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut ct)
        .unwrap();
    let mut body = nonce.to_vec();
    body.extend_from_slice(&ct);
    let mut dst: Vec<u8> = Vec::with_capacity(size + 16);
    let mut ok = 0usize;
    let t = Instant::now();
    for _ in 0..n {
        dst.clear();
        dst.extend_from_slice(&body[12..]);
        let nonce = Nonce::try_assume_unique_for_key(&body[..12]).unwrap();
        if let Ok(pt) = key.open_in_place(nonce, Aad::empty(), &mut dst) {
            if pt.len() == size {
                ok += 1;
            }
        }
    }
    let el = t.elapsed();
    println!(
        "rust ring::aead AES_256_GCM size={} ns/open={:.1} GB/s={:.2} ok={}",
        size,
        el.as_nanos() as f64 / n as f64,
        (n * size) as f64 / el.as_secs_f64() / 1e9,
        ok
    );
}
