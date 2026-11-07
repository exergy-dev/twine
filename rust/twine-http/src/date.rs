//! The cached `Date` header line.
//!
//! One [`DateCache`] lives in a per-loop `LoopLocal`. The design calls for a
//! `call_later` timer that refreshes it once per second; this crate refreshes
//! lazily on use instead: [`DateCache::get`] compares the loop's cached
//! `Instant` with the boundary of the second the line was generated in and
//! regenerates when it has passed. The observable behaviour is the same (the
//! line changes at most once per second and the hot path does one `Instant`
//! comparison), it needs no timer to arm, re-arm or retire, and an idle loop
//! does no work.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Length of an IMF-fixdate such as `Sun, 06 Nov 1994 08:49:37 GMT`.
pub const HTTP_DATE_LEN: usize = 29;
/// Length of the cached header line: `Date: ` + date + CRLF.
const LINE_LEN: usize = 6 + HTTP_DATE_LEN + 2;

const DAYS: [&[u8; 3]; 7] = [b"Thu", b"Fri", b"Sat", b"Sun", b"Mon", b"Tue", b"Wed"];
const MONTHS: [&[u8; 3]; 12] = [
    b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov", b"Dec",
];

/// Converts days since 1970-01-01 to a (year, month 1..=12, day 1..=31) civil
/// date (Howard Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn put2(out: &mut [u8], v: u32) {
    out[0] = b'0' + (v / 10) as u8;
    out[1] = b'0' + (v % 10) as u8;
}

/// Formats `unix_secs` (seconds since the epoch, UTC) as an IMF-fixdate into
/// `out`, e.g. `Sun, 06 Nov 1994 08:49:37 GMT`. Years above 9999 are clamped.
pub fn format_http_date(unix_secs: u64, out: &mut [u8; HTTP_DATE_LEN]) {
    let days = (unix_secs / 86_400) as i64;
    let rem = (unix_secs % 86_400) as u32;
    let (y, m, d) = civil_from_days(days);
    let y = y.clamp(0, 9999) as u32;
    out[0..3].copy_from_slice(DAYS[(days.rem_euclid(7)) as usize]);
    out[3] = b',';
    out[4] = b' ';
    put2(&mut out[5..7], d);
    out[7] = b' ';
    out[8..11].copy_from_slice(MONTHS[(m - 1) as usize]);
    out[11] = b' ';
    put2(&mut out[12..14], y / 100);
    put2(&mut out[14..16], y % 100);
    out[16] = b' ';
    put2(&mut out[17..19], rem / 3600);
    out[19] = b':';
    put2(&mut out[20..22], (rem / 60) % 60);
    out[22] = b':';
    put2(&mut out[23..25], rem % 60);
    out[25..29].copy_from_slice(b" GMT");
}

/// The per-loop cached `Date: ...\r\n` line.
pub(crate) struct DateCache {
    line: [u8; LINE_LEN],
    /// Instant at (or after) which the line must be regenerated.
    next: Option<Instant>,
}

impl DateCache {
    pub(crate) fn new() -> DateCache {
        let mut line = [0u8; LINE_LEN];
        line[..6].copy_from_slice(b"Date: ");
        line[LINE_LEN - 2..].copy_from_slice(b"\r\n");
        DateCache { line, next: None }
    }

    fn refresh(&mut self, now: Instant) {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let mut d = [0u8; HTTP_DATE_LEN];
        format_http_date(wall.as_secs(), &mut d);
        self.line[6..6 + HTTP_DATE_LEN].copy_from_slice(&d);
        // The boundary is anchored at `now`, which is never later than the
        // wall-clock sample, so the next refresh can be early, never late.
        self.next = Some(now + Duration::from_nanos(1_000_000_000 - wall.subsec_nanos() as u64));
    }

    /// The current line, regenerated when `now` crossed a second boundary.
    pub(crate) fn get(&mut self, now: Instant) -> &[u8] {
        if self.next.is_none_or(|n| now >= n) {
            self.refresh(now);
        }
        &self.line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fmt(s: u64) -> String {
        let mut b = [0u8; HTTP_DATE_LEN];
        format_http_date(s, &mut b);
        String::from_utf8(b.to_vec()).unwrap()
    }

    #[test]
    fn known_dates() {
        assert_eq!(fmt(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(fmt(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(fmt(951_782_400), "Tue, 29 Feb 2000 00:00:00 GMT");
        assert_eq!(fmt(1_709_210_096), "Thu, 29 Feb 2024 12:34:56 GMT");
        assert_eq!(fmt(4_102_444_799), "Thu, 31 Dec 2099 23:59:59 GMT");
        assert_eq!(fmt(1_000_000_000), "Sun, 09 Sep 2001 01:46:40 GMT");
    }

    #[test]
    fn cache_refreshes_after_a_second() {
        let mut c = DateCache::new();
        let t0 = Instant::now();
        let a = c.get(t0).to_vec();
        assert!(a.starts_with(b"Date: ") && a.ends_with(b"\r\n") && a.len() == LINE_LEN);
        let n = c.next.unwrap();
        assert!(n > t0 && n <= t0 + Duration::from_secs(1));
        // Before the boundary the cache is not regenerated.
        c.next = Some(t0 + Duration::from_secs(3600));
        c.get(t0);
        assert_eq!(c.next, Some(t0 + Duration::from_secs(3600)));
        c.get(t0 + Duration::from_secs(3601));
        assert_ne!(c.next, Some(t0 + Duration::from_secs(3600)));
    }
}
