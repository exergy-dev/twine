//! Shared helpers for the twine benchmark binaries: flag parsing, CPU-list
//! parsing and an HTTP date formatter for the tokio baseline. No `unsafe` here.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::time::Duration;

/// Parsed `--name value` / `--name=value` / `-c value` flags plus positionals.
pub struct Args {
    flags: HashMap<String, String>,
    /// Arguments that are not flags, in order.
    pub positional: Vec<String>,
}

impl Args {
    /// Parses `std::env::args`. `known` lists accepted flag names (no dashes);
    /// `-h`/`--help` prints `usage` and exits 0, anything unknown exits 2.
    pub fn parse(usage: &str, known: &[&str]) -> Args {
        let mut flags = HashMap::new();
        let mut positional = Vec::new();
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            if a == "-h" || a == "--help" {
                println!("{usage}");
                std::process::exit(0);
            }
            let Some(rest) = a.strip_prefix("--").or_else(|| a.strip_prefix('-')) else {
                positional.push(a);
                continue;
            };
            let (k, v) = match rest.split_once('=') {
                Some((k, v)) => (k.to_string(), v.to_string()),
                None => match it.next() {
                    Some(v) => (rest.to_string(), v),
                    None => fail(usage, &format!("flag {a} needs a value")),
                },
            };
            if !known.contains(&k.as_str()) {
                fail(usage, &format!("unknown flag {a}"));
            }
            flags.insert(k, v);
        }
        Args { flags, positional }
    }

    /// The flag's value, if given.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.flags.get(name).map(String::as_str)
    }

    /// The flag's value or `default`.
    pub fn str(&self, name: &str, default: &str) -> String {
        self.get(name).unwrap_or(default).to_string()
    }

    /// The flag as an integer (exits 2 on a parse error).
    pub fn usize(&self, name: &str, default: usize) -> usize {
        self.parse_or(name, default, |v| v.parse().ok())
    }

    /// The flag as a float.
    pub fn f64(&self, name: &str, default: f64) -> f64 {
        self.parse_or(name, default, |v| v.parse().ok())
    }

    /// The flag as a duration (`10s`, `500ms`, `2m`; a bare number is seconds).
    pub fn duration(&self, name: &str, default: Duration) -> Duration {
        self.parse_or(name, default, parse_duration)
    }

    /// The flag as a CPU list (`0,2,4-7`), empty when absent.
    pub fn cpus(&self, name: &str) -> Vec<usize> {
        self.parse_or(name, Vec::new(), parse_cpus)
    }

    fn parse_or<T>(&self, name: &str, default: T, f: impl Fn(&str) -> Option<T>) -> T {
        match self.get(name) {
            None => default,
            Some(v) => f(v).unwrap_or_else(|| {
                eprintln!("bad value {v:?} for --{name}");
                std::process::exit(2)
            }),
        }
    }
}

fn fail(usage: &str, msg: &str) -> ! {
    eprintln!("{msg}\n{usage}");
    std::process::exit(2)
}

/// Parses `1.5s`, `250ms`, `2m`, `1h`, or a bare number of seconds.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 0.001)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60.0)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600.0)
    } else {
        (s, 1.0)
    };
    let v: f64 = num.parse().ok()?;
    if !v.is_finite() || v < 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(v * mult).ok()
}

/// Parses a CPU list such as `0,2,4-7`.
pub fn parse_cpus(s: &str) -> Option<Vec<usize>> {
    let mut out = Vec::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                let (a, b): (usize, usize) = (a.parse().ok()?, b.parse().ok()?);
                if a > b {
                    return None;
                }
                out.extend(a..=b);
            }
            None => out.push(part.parse().ok()?),
        }
    }
    Some(out)
}

/// Formats `unix_secs` as an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`, 29 bytes).
pub fn http_date(unix_secs: u64) -> [u8; 29] {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = (unix_secs / 86400) as i64;
    let rem = unix_secs % 86400;
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let s = format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        DAYS[(days % 7) as usize],
        d,
        MONTHS[(m - 1) as usize],
        y,
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    );
    let mut out = [0u8; 29];
    out.copy_from_slice(s.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date() {
        assert_eq!(&http_date(784_111_777), b"Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(&http_date(0), b"Thu, 01 Jan 1970 00:00:00 GMT");
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        for bad in ["inf", "NaN", "-1s", "1e400", "1e30h", "x"] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn cpus() {
        assert_eq!(parse_cpus("0,2-4"), Some(vec![0, 2, 3, 4]));
        assert_eq!(parse_cpus("x"), None);
    }
}
