//! Batched UDP I/O (`recvmmsg`/`sendmmsg` on Linux, a `recv_from`/`send_to`
//! loop elsewhere).

use std::io;
use std::net::SocketAddr;

use libc::socklen_t;

use crate::{from_sockaddr, to_sockaddr};

/// Batched UDP. Each message has a payload buffer of `buf_size` bytes and
/// address storage. All memory is allocated in [`Batch::new`]; the per-call
/// paths allocate nothing. Not thread-safe.
pub struct Batch {
    n: usize,
    buf_size: usize,
    /// One contiguous slab, `n` chunks of `buf_size` bytes.
    slab: Box<[u8]>,
    /// Payload length of each message (bytes to send / bytes received).
    lens: Box<[usize]>,
    addrs: Box<[libc::sockaddr_storage]>,
    /// Valid bytes in `addrs[i]`; 0 means "no address".
    addr_lens: Box<[socklen_t]>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    hdrs: Box<[libc::mmsghdr]>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    iovs: Box<[libc::iovec]>,
}

// SAFETY: the raw pointers inside `hdrs`/`iovs` point only into heap memory
// owned by this same Batch (and are rewritten before every syscall), so
// moving the Batch to another thread is sound.
#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe impl Send for Batch {}

impl Batch {
    /// Creates a batch of `n` messages with `buf_size`-byte payload buffers.
    pub fn new(n: usize, buf_size: usize) -> Batch {
        // SAFETY: all-zero is a valid sockaddr_storage.
        let zero_ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        Batch {
            n,
            buf_size,
            slab: vec![0u8; n.checked_mul(buf_size).expect("Batch: n * buf_size overflows")]
                .into_boxed_slice(),
            lens: vec![buf_size; n].into_boxed_slice(),
            addrs: vec![zero_ss; n].into_boxed_slice(),
            addr_lens: vec![0; n].into_boxed_slice(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            // SAFETY: all-zero is a valid mmsghdr (null pointers, zero lengths).
            hdrs: (0..n).map(|_| unsafe { std::mem::zeroed() }).collect(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            iovs: vec![
                libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0
                };
                n
            ]
            .into_boxed_slice(),
        }
    }

    /// Number of messages (`n`).
    pub fn len(&self) -> usize {
        self.n
    }

    /// True when the batch holds no messages.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Payload buffer of message `i` (full capacity) for filling before send.
    pub fn buf_mut(&mut self, i: usize) -> &mut [u8] {
        assert!(i < self.n, "message index out of range");
        &mut self.slab[i * self.buf_size..(i + 1) * self.buf_size]
    }

    /// Sets the payload length of message `i` (bytes to send), clamped to capacity.
    pub fn set_len(&mut self, i: usize, n: usize) {
        self.lens[i] = n.min(self.buf_size);
    }

    /// Sets the destination of message `i`. Does not allocate.
    pub fn set_addr(&mut self, i: usize, addr: SocketAddr) {
        self.addr_lens[i] = to_sockaddr(addr, &mut self.addrs[i]);
    }

    fn addr_of(&self, i: usize) -> Option<SocketAddr> {
        match self.addr_lens[i] {
            0 => None,
            l => from_sockaddr(&self.addrs[i], l),
        }
    }

    /// Received message `i`: payload (length = bytes received) and source address.
    pub fn msg(&self, i: usize) -> (&[u8], Option<SocketAddr>) {
        let lo = i * self.buf_size;
        (&self.slab[lo..lo + self.lens[i]], self.addr_of(i))
    }

    /// Mutable form of [`Batch::msg`].
    pub fn msg_mut(&mut self, i: usize) -> (&mut [u8], Option<SocketAddr>) {
        let a = self.addr_of(i);
        let lo = i * self.buf_size;
        (&mut self.slab[lo..lo + self.lens[i]], a)
    }

    /// Receives up to `n` messages. `WouldBlock` when nothing is readable.
    /// Resets every message's length to capacity before the call.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn recv_mmsg(&mut self, fd: i32) -> io::Result<usize> {
        // Base pointers taken once: storing a pointer derived from a fresh
        // `&mut self.x[i]` each iteration would retag the whole slice and
        // invalidate the pointers stored for earlier elements.
        let slab = self.slab.as_mut_ptr();
        let iovs = self.iovs.as_mut_ptr();
        let addrs = self.addrs.as_mut_ptr();
        for i in 0..self.n {
            self.lens[i] = self.buf_size;
            // SAFETY: i < n, so every offset is inside the respective Box<[T]>.
            let (iov, addr) = unsafe { (&mut *iovs.add(i), addrs.add(i)) };
            // SAFETY: i * buf_size < n * buf_size == slab.len().
            iov.iov_base = unsafe { slab.add(i * self.buf_size) }.cast();
            iov.iov_len = self.buf_size;
            let h = &mut self.hdrs[i].msg_hdr;
            h.msg_name = addr.cast();
            h.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as socklen_t;
            h.msg_iov = iov;
            h.msg_iovlen = 1;
            h.msg_flags = 0;
            self.hdrs[i].msg_len = 0;
            self.addr_lens[i] = 0;
        }
        if self.n == 0 {
            return Ok(0);
        }
        let got = loop {
            // SAFETY: hdrs holds `n` initialised mmsghdr whose name/iov
            // pointers refer to live memory owned by self.
            let r = unsafe {
                libc::recvmmsg(
                    fd,
                    self.hdrs.as_mut_ptr(),
                    self.n as _,
                    libc::MSG_DONTWAIT as _,
                    std::ptr::null_mut(),
                )
            };
            if r >= 0 {
                break r as usize;
            }
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EINTR) {
                return Err(e);
            }
        };
        for i in 0..got {
            self.lens[i] = (self.hdrs[i].msg_len as usize).min(self.buf_size);
            self.addr_lens[i] = self.hdrs[i].msg_hdr.msg_namelen;
        }
        Ok(got)
    }

    /// Sends the first `n` messages; returns how many were sent.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn send_mmsg(&mut self, fd: i32, n: usize) -> io::Result<usize> {
        let n = n.min(self.n);
        if n == 0 {
            return Ok(0);
        }
        let slab = self.slab.as_mut_ptr();
        let iovs = self.iovs.as_mut_ptr();
        let addrs = self.addrs.as_mut_ptr();
        for i in 0..n {
            // SAFETY: i < n <= self.n, so every offset is inside its Box<[T]>
            // (see recv_mmsg for why base pointers are taken once).
            let (iov, addr) = unsafe { (&mut *iovs.add(i), addrs.add(i)) };
            // SAFETY: i * buf_size < n * buf_size == slab.len().
            iov.iov_base = unsafe { slab.add(i * self.buf_size) }.cast();
            iov.iov_len = self.lens[i];
            let h = &mut self.hdrs[i].msg_hdr;
            if self.addr_lens[i] > 0 {
                h.msg_name = addr.cast();
                h.msg_namelen = self.addr_lens[i];
            } else {
                h.msg_name = std::ptr::null_mut();
                h.msg_namelen = 0;
            }
            h.msg_iov = iov;
            h.msg_iovlen = 1;
            h.msg_flags = 0;
            self.hdrs[i].msg_len = 0;
        }
        loop {
            // SAFETY: the first `n` mmsghdr are initialised and point at live
            // memory owned by self.
            let r = unsafe {
                libc::sendmmsg(fd, self.hdrs.as_mut_ptr(), n as _, libc::MSG_DONTWAIT as _)
            };
            if r >= 0 {
                return Ok(r as usize);
            }
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EINTR) {
                return Err(e);
            }
        }
    }

    /// Receives up to `n` messages (portable `recv_from` loop). `WouldBlock`
    /// when nothing is readable. Resets every message's length first.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn recv_mmsg(&mut self, fd: i32) -> io::Result<usize> {
        for i in 0..self.n {
            self.lens[i] = self.buf_size;
            self.addr_lens[i] = 0;
        }
        let mut got = 0;
        while got < self.n {
            let lo = got * self.buf_size;
            match crate::recv_from(fd, &mut self.slab[lo..lo + self.buf_size]) {
                Ok((len, a)) => {
                    self.lens[got] = len;
                    self.addr_lens[got] = to_sockaddr(a, &mut self.addrs[got]);
                    got += 1;
                }
                Err(e) if got > 0 && e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if got > 0 => {
                    let _ = e;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(got)
    }

    /// Sends the first `n` messages (portable `send_to` loop); returns how many were sent.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn send_mmsg(&mut self, fd: i32, n: usize) -> io::Result<usize> {
        let n = n.min(self.n);
        let mut sent = 0;
        while sent < n {
            let lo = sent * self.buf_size;
            let payload = &self.slab[lo..lo + self.lens[sent]];
            let r = match self.addr_of(sent) {
                Some(a) => crate::send_to(fd, payload, a),
                // SAFETY: payload is a live slice; connected-socket send.
                None => loop {
                    let r = unsafe { libc::send(fd, payload.as_ptr().cast(), payload.len(), 0) };
                    if r >= 0 {
                        break Ok(r as usize);
                    }
                    let e = io::Error::last_os_error();
                    if e.raw_os_error() != Some(libc::EINTR) {
                        break Err(e);
                    }
                },
            };
            match r {
                Ok(_) => sent += 1,
                Err(e) if sent == 0 => return Err(e),
                Err(_) => break,
            }
        }
        Ok(sent)
    }
}
