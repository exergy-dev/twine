//! TUN device open and write (libc).

use std::io;

/// Attaches to an existing TUN interface and returns a non-blocking fd
/// delivering raw IP packets (IFF_TUN | IFF_NO_PI).
pub fn open(name: &str) -> io::Result<i32> {
    if name.len() >= libc::IFNAMSIZ {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name too long",
        ));
    }
    // SAFETY: the path is a valid NUL-terminated string.
    let fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: ifreq is plain data; all-zero is a valid value.
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    for (d, s) in ifr.ifr_name.iter_mut().zip(name.bytes()) {
        *d = s as libc::c_char;
    }
    ifr.ifr_ifru.ifru_flags = (libc::IFF_TUN | libc::IFF_NO_PI) as libc::c_short;
    // SAFETY: fd is open and ifr is a valid, initialised ifreq that TUNSETIFF reads.
    let r = unsafe { libc::ioctl(fd, libc::TUNSETIFF as _, &mut ifr as *mut libc::ifreq) };
    if r < 0 {
        let e = io::Error::last_os_error();
        // SAFETY: fd was opened above and is not used afterwards.
        unsafe { libc::close(fd) };
        return Err(e);
    }
    // SAFETY: fd is open; F_GETFL/F_SETFL take and return plain ints.
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    // SAFETY: as above.
    if fl < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK) } < 0 {
        let e = io::Error::last_os_error();
        // SAFETY: fd was opened above and is not used afterwards.
        unsafe { libc::close(fd) };
        return Err(e);
    }
    Ok(fd)
}

/// Writes one packet. A non-blocking TUN write never blocks: EAGAIN means the
/// kernel queue is full and the packet is dropped.
pub fn write(fd: i32, pkt: &[u8]) -> bool {
    // SAFETY: pkt is a valid readable buffer of pkt.len() bytes for the call.
    let n = unsafe { libc::write(fd, pkt.as_ptr().cast(), pkt.len()) };
    n >= 0
}

pub fn close(fd: i32) {
    // SAFETY: the caller owns fd and does not use it afterwards.
    unsafe { libc::close(fd) };
}
