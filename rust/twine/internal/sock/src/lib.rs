//! Twine internal crate `twine-sock`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
//!
//! Raw, non-blocking socket helpers: listen/accept/connect, socket options,
//! address conversion and batched UDP I/O. Every socket created or accepted
//! here is `SOCK_NONBLOCK | SOCK_CLOEXEC` (via `accept4`/`socket` flags where
//! the platform has them, `fcntl` otherwise). File descriptors are plain
//! `i32`; the caller owns their lifetimes.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

use std::io;
use std::mem::{size_of, zeroed};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;

use libc::{c_int, c_void, socklen_t};

/// True on platforms where `socket`/`accept4` take `SOCK_NONBLOCK|SOCK_CLOEXEC`.
const HAVE_SOCK_FLAGS: bool = cfg!(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd"
));

mod batch;
pub use batch::Batch;

// ---------------------------------------------------------------- helpers

#[inline]
fn last_errno() -> c_int {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// Converts a `-1` syscall result into the current OS error.
#[inline]
fn cvt(r: c_int) -> io::Result<c_int> {
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

/// Like [`cvt`] for `ssize_t` results, retrying EINTR through `f`.
#[inline]
fn retry_isize(mut f: impl FnMut() -> isize) -> io::Result<usize> {
    loop {
        let r = f();
        if r >= 0 {
            return Ok(r as usize);
        }
        if last_errno() != libc::EINTR {
            return Err(io::Error::last_os_error());
        }
    }
}

fn setsockopt_int(fd: i32, level: c_int, name: c_int, val: c_int) -> io::Result<()> {
    // SAFETY: `val` is a live c_int and the length passed is its size.
    let r = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&val as *const c_int).cast::<c_void>(),
            size_of::<c_int>() as socklen_t,
        )
    };
    cvt(r).map(drop)
}

fn getsockopt_int(fd: i32, level: c_int, name: c_int) -> io::Result<c_int> {
    let mut val: c_int = 0;
    let mut len = size_of::<c_int>() as socklen_t;
    // SAFETY: `val`/`len` are live locals, len is the size of val.
    let r = unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            (&mut val as *mut c_int).cast::<c_void>(),
            &mut len,
        )
    };
    cvt(r)?;
    Ok(val)
}

/// Marks a freshly created or accepted fd non-blocking and close-on-exec when
/// the syscall itself could not, and suppresses SIGPIPE where the OS has a
/// per-socket knob.
fn finish_fd(fd: i32) -> io::Result<()> {
    if !HAVE_SOCK_FLAGS {
        set_nonblock(fd)?;
        // SAFETY: plain fcntl on an fd we own.
        cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) })?;
    }
    #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
    {
        let _ = setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_NOSIGPIPE, 1);
    }
    Ok(())
}

fn new_socket(addr: &SocketAddr, ty: c_int) -> io::Result<i32> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    let flags = if HAVE_SOCK_FLAGS {
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
        {
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC
        }
        #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
        {
            0
        }
    } else {
        0
    };
    // SAFETY: plain socket(2) call with integer arguments.
    let fd = cvt(unsafe { libc::socket(domain, ty | flags, 0) })?;
    if let Err(e) = finish_fd(fd) {
        close(fd);
        return Err(e);
    }
    Ok(fd)
}

fn bind_addr(fd: i32, addr: SocketAddr) -> io::Result<()> {
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let len = to_sockaddr(addr, &mut ss);
    // SAFETY: `ss` holds `len` valid bytes of a sockaddr.
    cvt(unsafe { libc::bind(fd, (&ss as *const libc::sockaddr_storage).cast(), len) }).map(drop)
}

// ------------------------------------------------------------ address casts

/// Fills a `sockaddr_storage` from a `SocketAddr`; returns the length. No alloc.
pub fn to_sockaddr(addr: SocketAddr, out: &mut libc::sockaddr_storage) -> socklen_t {
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    *out = unsafe { zeroed() };
    match addr {
        SocketAddr::V4(a) => {
            // SAFETY: sockaddr_storage is at least as large and as aligned as
            // sockaddr_in, and all-zero is a valid sockaddr_in.
            let sin =
                unsafe { &mut *(out as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            #[cfg(any(
                target_vendor = "apple",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly"
            ))]
            {
                sin.sin_len = size_of::<libc::sockaddr_in>() as u8;
            }
            sin.sin_port = a.port().to_be();
            sin.sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
            size_of::<libc::sockaddr_in>() as socklen_t
        }
        SocketAddr::V6(a) => {
            // SAFETY: as above for sockaddr_in6.
            let sin6 =
                unsafe { &mut *(out as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            #[cfg(any(
                target_vendor = "apple",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly"
            ))]
            {
                sin6.sin6_len = size_of::<libc::sockaddr_in6>() as u8;
            }
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_addr.s6_addr = a.ip().octets();
            sin6.sin6_scope_id = a.scope_id();
            size_of::<libc::sockaddr_in6>() as socklen_t
        }
    }
}

/// Converts a `sockaddr_storage` (`len` bytes valid) back; `None` for other families.
pub fn from_sockaddr(sa: &libc::sockaddr_storage, len: socklen_t) -> Option<SocketAddr> {
    let len = len as usize;
    match sa.ss_family as c_int {
        libc::AF_INET if len >= size_of::<libc::sockaddr_in>() => {
            // SAFETY: family is AF_INET and `len` covers a whole sockaddr_in
            // inside the (large enough, aligned) storage.
            let sin =
                unsafe { &*(sa as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Some(SocketAddr::V4(SocketAddrV4::new(
                ip,
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 if len >= size_of::<libc::sockaddr_in6>() => {
            // SAFETY: as above for sockaddr_in6.
            let sin6 =
                unsafe { &*(sa as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>() };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(sin6.sin6_addr.s6_addr),
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

// ----------------------------------------------------------- listen/accept

/// Creates, binds and listens a non-blocking TCP socket. SO_REUSEADDR is
/// always set; SO_REUSEPORT when `reuse_port`. Returns the fd and the bound
/// address (so port 0 is resolved). Binds exactly the given family and leaves
/// IPV6_V6ONLY at the kernel default; the core resolves "host:port" strings.
pub fn listen_tcp(
    addr: SocketAddr,
    reuse_port: bool,
    backlog: i32,
) -> io::Result<(i32, SocketAddr)> {
    let fd = new_socket(&addr, libc::SOCK_STREAM)?;
    let r = (|| {
        setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1)?;
        if reuse_port {
            set_reuse_port(fd, true)?;
        }
        bind_addr(fd, addr)?;
        let backlog = if backlog <= 0 {
            libc::SOMAXCONN
        } else {
            backlog
        };
        // SAFETY: plain listen(2) on an fd we own.
        cvt(unsafe { libc::listen(fd, backlog) })?;
        local_addr(fd)
    })();
    match r {
        Ok(la) => Ok((fd, la)),
        Err(e) => {
            close(fd);
            Err(e)
        }
    }
}

/// Binds a non-blocking UDP socket, same address rules. SO_REUSEADDR is not
/// set (on UDP it would let unrelated sockets share the port).
pub fn listen_udp(addr: SocketAddr, reuse_port: bool) -> io::Result<(i32, SocketAddr)> {
    let fd = new_socket(&addr, libc::SOCK_DGRAM)?;
    let r = (|| {
        if reuse_port {
            set_reuse_port(fd, true)?;
        }
        bind_addr(fd, addr)?;
        local_addr(fd)
    })();
    match r {
        Ok(la) => Ok((fd, la)),
        Err(e) => {
            close(fd);
            Err(e)
        }
    }
}

/// `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` (accept + fcntl where unavailable).
/// Returns `WouldBlock` when the backlog is empty; EINTR and ECONNABORTED are
/// retried internally. Does not allocate (the `SocketAddr` is built from a
/// stack `sockaddr_storage`).
/// `EMFILE`/`ENFILE` are returned as errors with the connection still in the
/// backlog; under edge-triggered polling the caller must retry later (for
/// example from a timer) because no new readiness edge will arrive for it.
pub fn accept(lfd: i32) -> io::Result<(i32, SocketAddr)> {
    loop {
        // SAFETY: an all-zero sockaddr_storage is a valid value.
        let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
        let mut len = size_of::<libc::sockaddr_storage>() as socklen_t;
        let sa = (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr>();
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
        // SAFETY: `sa`/`len` describe the live, writable `ss`.
        let fd =
            unsafe { libc::accept4(lfd, sa, &mut len, libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) };
        #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
        // SAFETY: `sa`/`len` describe the live, writable `ss`.
        let fd = unsafe { libc::accept(lfd, sa, &mut len) };
        if fd < 0 {
            let e = last_errno();
            if e == libc::EINTR || e == libc::ECONNABORTED {
                continue;
            }
            return Err(io::Error::from_raw_os_error(e));
        }
        if let Err(e) = finish_fd(fd) {
            close(fd);
            return Err(e);
        }
        return match from_sockaddr(&ss, len) {
            Some(a) => Ok((fd, a)),
            None => {
                close(fd);
                Err(io::Error::from_raw_os_error(libc::EAFNOSUPPORT))
            }
        };
    }
}

/// Creates a non-blocking socket, optionally binds `local`, and starts
/// connecting. `in_progress == true` for EINPROGRESS (the common case): the
/// caller waits for WRITABLE and calls [`connect_error`]. On error the socket
/// is closed.
pub fn connect(addr: SocketAddr, local: Option<SocketAddr>) -> io::Result<(i32, bool)> {
    let fd = new_socket(&addr, libc::SOCK_STREAM)?;
    if let Some(l) = local {
        if let Err(e) = bind_addr(fd, l) {
            close(fd);
            return Err(e);
        }
    }
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let len = to_sockaddr(addr, &mut ss);
    // SAFETY: `ss` holds `len` valid bytes of a sockaddr.
    let r = unsafe { libc::connect(fd, (&ss as *const libc::sockaddr_storage).cast(), len) };
    if r == 0 {
        return Ok((fd, false));
    }
    match last_errno() {
        // EINTR on a non-blocking connect: the handshake continues asynchronously.
        libc::EINPROGRESS | libc::EALREADY | libc::EINTR => Ok((fd, true)),
        e => {
            close(fd);
            Err(io::Error::from_raw_os_error(e))
        }
    }
}

/// Reads SO_ERROR after a non-blocking connect completes. `Ok(())` = connected.
pub fn connect_error(fd: i32) -> io::Result<()> {
    match so_error(fd)? {
        None => Ok(()),
        Some(e) => Err(io::Error::from_raw_os_error(e)),
    }
}

// ---------------------------------------------------------------- options

/// Sets TCP_NODELAY.
pub fn set_nodelay(fd: i32, on: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, on as c_int)
}

/// Sets TCP_DEFER_ACCEPT to `d` (whole seconds, rounded up) on Linux; `Ok(())` elsewhere.
pub fn set_defer_accept(fd: i32, d: Duration) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let secs = d.as_secs() + u64::from(d.subsec_nanos() > 0);
        setsockopt_int(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_DEFER_ACCEPT,
            secs.min(c_int::MAX as u64) as c_int,
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (fd, d);
        Ok(())
    }
}

/// Sets SO_BUSY_POLL to `d` (microseconds) on Linux; `Ok(())` elsewhere.
pub fn set_busy_poll(fd: i32, d: Duration) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let us = d.as_micros().min(c_int::MAX as u128) as c_int;
        setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_BUSY_POLL, us)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (fd, d);
        Ok(())
    }
}

/// Sets SO_RCVBUF to `rcv` and SO_SNDBUF to `snd`; 0 leaves that buffer at its default.
pub fn set_buffers(fd: i32, rcv: usize, snd: usize) -> io::Result<()> {
    if rcv > 0 {
        setsockopt_int(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            rcv.min(c_int::MAX as usize) as c_int,
        )?;
    }
    if snd > 0 {
        setsockopt_int(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            snd.min(c_int::MAX as usize) as c_int,
        )?;
    }
    Ok(())
}

/// Sets SO_KEEPALIVE.
pub fn set_keepalive(fd: i32, on: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, on as c_int)
}

/// Puts `fd` into non-blocking mode.
pub fn set_nonblock(fd: i32) -> io::Result<()> {
    // SAFETY: plain fcntl calls with integer arguments.
    let fl = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    if fl & libc::O_NONBLOCK == 0 {
        // SAFETY: as above.
        cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK) })?;
    }
    Ok(())
}

/// Sets SO_REUSEPORT.
pub fn set_reuse_port(fd: i32, on: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, on as c_int)
}

fn name_of(
    fd: i32,
    f: unsafe extern "C" fn(c_int, *mut libc::sockaddr, *mut socklen_t) -> c_int,
) -> io::Result<SocketAddr> {
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let mut len = size_of::<libc::sockaddr_storage>() as socklen_t;
    // SAFETY: pointer and length describe the live, writable `ss`.
    cvt(unsafe {
        f(
            fd,
            (&mut ss as *mut libc::sockaddr_storage).cast(),
            &mut len,
        )
    })?;
    from_sockaddr(&ss, len).ok_or_else(|| io::Error::from_raw_os_error(libc::EAFNOSUPPORT))
}

/// `getsockname(2)`.
pub fn local_addr(fd: i32) -> io::Result<SocketAddr> {
    name_of(fd, libc::getsockname)
}

/// `getpeername(2)`.
pub fn peer_addr(fd: i32) -> io::Result<SocketAddr> {
    name_of(fd, libc::getpeername)
}

/// Reads and clears the pending SO_ERROR errno (`None` when zero).
pub fn so_error(fd: i32) -> io::Result<Option<i32>> {
    let v = getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_ERROR)?;
    Ok(if v == 0 { None } else { Some(v) })
}

// ------------------------------------------------------------------ data

/// `read(2)`; EINTR retried, EAGAIN surfaces as `WouldBlock`. Does not allocate.
pub fn read(fd: i32, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: pointer/length come from a live mutable slice.
    retry_isize(|| unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) as isize })
}

/// Largest iovec count passed to one `writev`/`sendmsg` (POSIX IOV_MAX floor).
pub const IOV_MAX: usize = 1024;

/// `MSG_NOSIGNAL` where the OS has it; Apple platforms use `SO_NOSIGPIPE`
/// (set in `finish_fd`) instead, so no flag is needed there.
#[cfg(not(target_vendor = "apple"))]
const NOSIGNAL: c_int = libc::MSG_NOSIGNAL;
#[cfg(target_vendor = "apple")]
const NOSIGNAL: c_int = 0;

/// Writes `buf`; EINTR retried, EAGAIN surfaces as `WouldBlock`. Does not
/// allocate. Uses `send(MSG_NOSIGNAL)` so a peer-closed socket yields `EPIPE`
/// instead of killing a process that has not ignored SIGPIPE (a library must
/// not rely on the binary's signal disposition); falls back to `write(2)` for
/// non-socket descriptors (`ENOTSOCK`).
pub fn write(fd: i32, buf: &[u8]) -> io::Result<usize> {
    // SAFETY: pointer/length come from a live slice.
    let r = retry_isize(|| unsafe {
        libc::send(fd, buf.as_ptr().cast(), buf.len(), NOSIGNAL) as isize
    });
    match r {
        Err(e) if e.raw_os_error() == Some(libc::ENOTSOCK) => {
            // SAFETY: pointer/length come from a live slice.
            retry_isize(|| unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) as isize })
        }
        other => other,
    }
}

/// Writes up to [`IOV_MAX`] iovecs with one `sendmsg(MSG_NOSIGNAL)`; EINTR
/// retried, EAGAIN surfaces as `WouldBlock`. Does not allocate. Falls back to
/// `writev(2)` for non-socket descriptors. The caller guarantees every iovec
/// points to live memory.
pub fn writev(fd: i32, iov: &[libc::iovec]) -> io::Result<usize> {
    let cnt = iov.len().min(IOV_MAX);
    if cnt == 0 {
        return Ok(0);
    }
    // SAFETY: all-zero is a valid msghdr; name/control stay null.
    let mut hdr: libc::msghdr = unsafe { std::mem::zeroed() };
    hdr.msg_iov = iov.as_ptr().cast_mut();
    hdr.msg_iovlen = cnt as _;
    // SAFETY: `hdr` points at a valid iovec array of `cnt` entries that
    // outlives the call; validity of each buffer is the caller's contract.
    let r = retry_isize(|| unsafe { libc::sendmsg(fd, &hdr, NOSIGNAL) as isize });
    match r {
        Err(e) if e.raw_os_error() == Some(libc::ENOTSOCK) => {
            // SAFETY: same iovec array, same contract.
            retry_isize(|| unsafe { libc::writev(fd, iov.as_ptr(), cnt as c_int) as isize })
        }
        other => other,
    }
}

/// `shutdown(fd, SHUT_WR)`.
pub fn shutdown_write(fd: i32) -> io::Result<()> {
    // SAFETY: plain shutdown(2) call.
    cvt(unsafe { libc::shutdown(fd, libc::SHUT_WR) }).map(drop)
}

/// `close(2)`, ignoring errors.
pub fn close(fd: i32) {
    // SAFETY: plain close(2); the caller owns `fd`.
    unsafe {
        libc::close(fd);
    }
}

/// `sendto(2)` of one datagram to `to`.
pub fn send_to(fd: i32, buf: &[u8], to: SocketAddr) -> io::Result<usize> {
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let len = to_sockaddr(to, &mut ss);
    let sa = (&ss as *const libc::sockaddr_storage).cast::<libc::sockaddr>();
    // SAFETY: buf is a live slice; `ss` holds `len` valid sockaddr bytes.
    retry_isize(|| unsafe { libc::sendto(fd, buf.as_ptr().cast(), buf.len(), 0, sa, len) as isize })
}

/// `recvfrom(2)` of one datagram, returning its length and source address.
pub fn recv_from(fd: i32, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: an all-zero sockaddr_storage is a valid value.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let mut len;
    let n = loop {
        len = size_of::<libc::sockaddr_storage>() as socklen_t;
        // SAFETY: buf and ss are live and writable with the stated sizes.
        let r = unsafe {
            libc::recvfrom(
                fd,
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
                (&mut ss as *mut libc::sockaddr_storage).cast(),
                &mut len,
            )
        };
        if r >= 0 {
            break r as usize;
        }
        if last_errno() != libc::EINTR {
            return Err(io::Error::last_os_error());
        }
    };
    let a =
        from_sockaddr(&ss, len).ok_or_else(|| io::Error::from_raw_os_error(libc::EAFNOSUPPORT))?;
    Ok((n, a))
}

#[cfg(test)]
mod tests;
