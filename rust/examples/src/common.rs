//! Stderr logger and argument parsing shared by every example binary.

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, m: &log::Metadata<'_>) -> bool {
        m.level() <= log::max_level()
    }
    fn log(&self, r: &log::Record<'_>) {
        if self.enabled(r.metadata()) {
            let d = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let secs = d.as_secs() % 86400;
            let _ = writeln!(
                std::io::stderr().lock(),
                "{:02}:{:02}:{:02}.{:03} {:5} {}",
                secs / 3600,
                secs / 60 % 60,
                secs % 60,
                d.subsec_millis(),
                r.level(),
                r.args()
            );
        }
    }
    fn flush(&self) {}
}

/// Installs the stderr logger at `Info` (or `Debug` with `RUST_LOG=debug`).
pub fn init_logger() {
    static L: StderrLogger = StderrLogger;
    let lvl = match std::env::var("RUST_LOG").as_deref() {
        Ok("debug") => log::LevelFilter::Debug,
        Ok("trace") => log::LevelFilter::Trace,
        Ok("warn") => log::LevelFilter::Warn,
        _ => log::LevelFilter::Info,
    };
    if log::set_logger(&L).is_ok() {
        log::set_max_level(lvl);
    }
}

/// Parsed `--name value` / `--name=value` flags.
pub struct Args {
    flags: HashMap<String, String>,
}

impl Args {
    /// Parses `std::env::args`. Prints `usage` and exits on `-h`/`--help` or an
    /// unknown flag (`known` lists the accepted names without dashes).
    pub fn parse(usage: &str, known: &[&str]) -> Args {
        let mut flags = HashMap::new();
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            if a == "-h" || a == "--help" {
                println!("{usage}");
                std::process::exit(0);
            }
            let Some(rest) = a.strip_prefix("--").or_else(|| a.strip_prefix('-')) else {
                die(usage, &format!("unexpected argument {a:?}"));
            };
            let (k, v) = match rest.split_once('=') {
                Some((k, v)) => (k.to_string(), v.to_string()),
                None => match it.next() {
                    Some(v) => (rest.to_string(), v),
                    None => die(usage, &format!("flag --{rest} needs a value")),
                },
            };
            if !known.contains(&k.as_str()) {
                die(usage, &format!("unknown flag --{k}"));
            }
            flags.insert(k, v);
        }
        Args { flags }
    }

    /// The flag's value or `default`.
    pub fn str(&self, name: &str, default: &str) -> String {
        self.flags
            .get(name)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    /// The flag as an integer (exits on a parse error).
    pub fn usize(&self, name: &str, default: usize) -> usize {
        match self.flags.get(name) {
            None => default,
            Some(v) => v.parse().unwrap_or_else(|_| {
                eprintln!("bad --{name} value {v:?}");
                std::process::exit(2)
            }),
        }
    }

    /// The flag as a duration such as `30s`, `500ms`, `2m`; a bare number is seconds.
    pub fn duration(&self, name: &str, default: Duration) -> Duration {
        match self.flags.get(name) {
            None => default,
            Some(v) => parse_duration(v).unwrap_or_else(|| {
                eprintln!(
                    "bad --{name} value {v:?} (want e.g. 500ms, 2s, 1.5m; finite, not negative)"
                );
                std::process::exit(2)
            }),
        }
    }
}

fn die(usage: &str, msg: &str) -> ! {
    eprintln!("{msg}\n{usage}");
    std::process::exit(2)
}

/// Parses `1.5s`, `250ms`, `2m`, `1h` or a bare number of seconds. `None` for
/// anything else, including negative, `NaN`, `inf` and out-of-range values.
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
    // Rejects negative, NaN, infinite and overflowing values without panicking.
    (v >= 0.0)
        .then(|| Duration::try_from_secs_f64(v * mult).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_accepts_units_and_rejects_garbage() {
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("3"), Some(Duration::from_secs(3)));
        for bad in [
            "inf", "-inf", "NaN", "nan", "-1s", "1e300h", "infs", "", "xs",
        ] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }
}
