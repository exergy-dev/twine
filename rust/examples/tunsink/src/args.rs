//! Go `flag`-style argument parsing: `-x v`, `-x=v`, `--x v`, bare booleans.

use std::time::Duration;

pub struct Config {
    pub tun: String,
    pub addr: String,
    pub brokers: String,
    pub topic: String,
    pub kafka_drop: bool,
    pub loops: usize,
    pub pin: Vec<usize>,
    pub direct: bool,
    pub sinks: usize,
    pub watchdog: Duration,
    pub stats: Duration,
    pub spin: i64,
}

const USAGE: &str = "usage: tunsink-rs [-tun tun0] [-addr :9000] [-brokers host:port,..] \
[-topic tun-packets] [-kafka-drop] [-loops 1] [-pin 2,4] [-mode channel|direct] \
[-sinks 1] [-watchdog 10ms] [-stats 1s] [-spin N]";

fn die(msg: &str) -> ! {
    eprintln!("{msg}\n{USAGE}");
    std::process::exit(2)
}

/// Parses a Go duration (`10ms`, `1s`, `1h30m`, `1.5s`, `0`). A bare number is
/// taken as seconds (more lenient than Go).
pub fn parse_duration(s: &str) -> Option<Duration> {
    if s.is_empty() {
        return None;
    }
    if let Ok(v) = s.parse::<f64>() {
        return (v >= 0.0)
            .then(|| Duration::try_from_secs_f64(v).ok())
            .flatten();
    }
    let mut rest = s;
    let mut total = 0.0f64;
    while !rest.is_empty() {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let num: f64 = rest[..end].parse().ok()?;
        rest = &rest[end..];
        let (mult, len) = if rest.starts_with("ms") {
            (0.001, 2)
        } else if rest.starts_with("us") {
            (1e-6, 2)
        } else if rest.starts_with("ns") {
            (1e-9, 2)
        } else if rest.starts_with('s') {
            (1.0, 1)
        } else if rest.starts_with('m') {
            (60.0, 1)
        } else if rest.starts_with('h') {
            (3600.0, 1)
        } else {
            return None;
        };
        rest = &rest[len..];
        total += num * mult;
    }
    Duration::try_from_secs_f64(total).ok()
}

pub fn parse() -> Config {
    let mut c = Config {
        tun: "tun0".into(),
        addr: ":9000".into(),
        brokers: String::new(),
        topic: "tun-packets".into(),
        kafka_drop: false,
        loops: 1,
        pin: Vec::new(),
        direct: false,
        sinks: 1,
        watchdog: Duration::from_millis(10),
        stats: Duration::from_secs(1),
        spin: -1,
    };
    let mut it = std::env::args().skip(1).peekable();
    while let Some(a) = it.next() {
        if a == "-h" || a == "--help" || a == "-help" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let Some(rest) = a.strip_prefix("--").or_else(|| a.strip_prefix('-')) else {
            die(&format!("unexpected argument {a:?}"));
        };
        let (name, inline) = match rest.split_once('=') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (rest, None),
        };
        if name == "kafka-drop" {
            c.kafka_drop = match inline.as_deref() {
                // strconv.ParseBool, as Go's flag package accepts.
                None | Some("1" | "t" | "T" | "true" | "TRUE" | "True") => true,
                Some("0" | "f" | "F" | "false" | "FALSE" | "False") => false,
                Some(v) => die(&format!("bad -kafka-drop value {v:?}")),
            };
            continue;
        }
        let val = match inline {
            Some(v) => v,
            None => it
                .next()
                .unwrap_or_else(|| die(&format!("flag -{name} needs a value"))),
        };
        let num = |v: &str| -> usize {
            v.parse()
                .unwrap_or_else(|_| die(&format!("bad -{name} value {v:?}")))
        };
        let dur = |v: &str| -> Duration {
            parse_duration(v).unwrap_or_else(|| die(&format!("bad -{name} value {v:?}")))
        };
        match name {
            "tun" => c.tun = val,
            "addr" => c.addr = val,
            "brokers" => c.brokers = val,
            "topic" => c.topic = val,
            "loops" => c.loops = num(&val),
            "pin" => {
                c.pin = val
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| {
                        s.trim()
                            .parse()
                            .unwrap_or_else(|_| die(&format!("-pin: bad cpu {s:?}")))
                    })
                    .collect()
            }
            "mode" => match val.as_str() {
                "direct" => c.direct = true,
                "channel" => c.direct = false,
                _ => {
                    eprintln!("unknown -mode {val:?}");
                    std::process::exit(1)
                }
            },
            "sinks" => c.sinks = num(&val).max(1),
            "watchdog" => c.watchdog = dur(&val),
            "stats" => c.stats = dur(&val),
            "spin" => {
                c.spin = val
                    .parse()
                    .unwrap_or_else(|_| die(&format!("bad -spin value {val:?}")))
            }
            _ => die(&format!("unknown flag -{name}")),
        }
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10ms"), Some(Duration::from_millis(10)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("1m30s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration("x"), None);
    }
}
