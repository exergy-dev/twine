//! Line-based chat room. Connect with any line-oriented client:
//!
//! ```text
//! cargo run --release -p twine-examples --bin chat -- --addr 0.0.0.0:9001
//! nc -C localhost 9001        # or: telnet localhost 9001
//! ```
//!
//! Every line a client sends is broadcast to every connected client, across all
//! loops. `/nick NAME` renames the sender and `/quit` disconnects.
//!
//! Fan-out: the room is a `twine_proto::Broadcast`. Each client adds its
//! `TransportHandle` in `connection_made` and removes its id in
//! `connection_lost`; a message is copied once into an `Arc<[u8]>` and
//! `Broadcast::send` posts one task per loop. Nicknames (at most 32 characters)
//! and lines have control characters removed, so a client cannot inject terminal
//! escape sequences or fake extra chat lines.

use std::sync::Arc;

use twine::{Engine, Error, ListenOptions, Transport};
use twine_examples::common::{init_logger, Args};
use twine_proto::{Broadcast, Flow, LineHandler, Lines};

const USAGE: &str = "usage: chat [--addr 0.0.0.0:9001] [--loops N]";
const MAX_NICK: usize = 32;

struct Room {
    bc: Broadcast,
}

/// Removes control characters (everything below 0x20, DEL and the C1 range).
fn clean(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

/// A sanitized nickname of at most [`MAX_NICK`] characters (may be empty).
fn clean_nick(s: &str) -> String {
    clean(s).trim().chars().take(MAX_NICK).collect()
}

struct Client {
    room: Arc<Room>,
    nick: String,
}

impl Client {
    /// Sends `text` plus CRLF to the whole room, including the sender.
    fn say(&self, text: String) {
        self.room.bc.send(format!("{text}\r\n").into_bytes());
    }
}

impl LineHandler for Client {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let others = self.room.bc.len();
        Lines::<Client>::send_line(
            t,
            format!(
                "* welcome {}; {others} other(s) here. /nick NAME to rename, /quit to leave",
                self.nick
            )
            .as_bytes(),
        );
        self.room.bc.add(t.handle());
        self.room
            .bc
            .send_from(format!("* {} joined\r\n", self.nick).into_bytes(), t.id());
    }

    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        let text = clean(&String::from_utf8_lossy(line));
        if let Some(n) = text.strip_prefix("/nick ") {
            let nick = clean_nick(n);
            if !nick.is_empty() {
                let old = std::mem::replace(&mut self.nick, nick.clone());
                self.say(format!("* {old} is now {nick}"));
            }
        } else if text == "/quit" {
            Lines::<Client>::send_line(t, b"* bye");
            t.lose_connection();
        } else if !text.trim().is_empty() {
            self.say(format!("<{}> {text}", self.nick));
        }
        Flow::Continue
    }

    fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
        self.room.bc.remove_id(t.id());
        let why = reason.map(|e| format!(" ({e})")).unwrap_or_default();
        self.say(format!("* {} left{why}", self.nick));
    }
}

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "loops"]);
    let eng = Engine::builder().loops(a.usize("loops", 0)).build()?;
    let room = Arc::new(Room {
        bc: Broadcast::new(),
    });
    let lst = eng.listen_tcp(
        a.str("addr", "0.0.0.0:9001").as_str(),
        twine::factory(move |_, peer| {
            Box::new(
                Lines::new(Client {
                    room: room.clone(),
                    nick: clean_nick(&peer.to_string()),
                })
                .with_max_line_length(1024),
            )
        }),
        ListenOptions::default(),
    )?;
    log::info!(
        "chat: listening on {} with {} loops",
        lst.addr(),
        eng.loop_handles().len()
    );
    eng.run()
}
