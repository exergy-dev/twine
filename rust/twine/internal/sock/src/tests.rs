use super::*;
use std::net::TcpStream;

fn lo4() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn wait_writable(fd: i32) {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: p is a live pollfd, count 1.
    let r = unsafe { libc::poll(&mut p, 1, 2000) };
    assert_eq!(r, 1, "poll timed out");
}

fn wait_readable(fd: i32) {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: p is a live pollfd, count 1.
    let r = unsafe { libc::poll(&mut p, 1, 2000) };
    assert_eq!(r, 1, "poll timed out");
}

fn is_nonblock_cloexec(fd: i32) -> bool {
    // SAFETY: plain fcntl queries.
    unsafe {
        libc::fcntl(fd, libc::F_GETFL) & libc::O_NONBLOCK != 0
            && libc::fcntl(fd, libc::F_GETFD) & libc::FD_CLOEXEC != 0
    }
}

#[test]
fn accept_returns_fd_and_peer() {
    let (l, la) = listen_tcp(lo4(), false, 0).unwrap();
    assert!(is_nonblock_cloexec(l));
    let c = TcpStream::connect(la).unwrap();
    let (fd, peer) = accept(l).unwrap();
    assert!(is_nonblock_cloexec(fd));
    assert_eq!(peer, c.local_addr().unwrap());
    assert_eq!(peer_addr(fd).unwrap(), peer);
    assert_eq!(local_addr(fd).unwrap(), la);
    close(fd);
    close(l);
}

#[test]
fn accept_would_block_when_idle() {
    let (l, _) = listen_tcp(lo4(), true, 16).unwrap();
    assert_eq!(accept(l).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    close(l);
}

#[test]
fn connect_completes() {
    let (l, la) = listen_tcp(lo4(), false, 0).unwrap();
    let (fd, _in_progress) = connect(la, None).unwrap();
    wait_writable(fd);
    connect_error(fd).unwrap();
    assert_eq!(so_error(fd).unwrap(), None);
    let (sfd, _) = accept(l).unwrap();
    set_nodelay(fd, true).unwrap();
    set_keepalive(fd, true).unwrap();
    set_buffers(fd, 0, 65536).unwrap();
    set_defer_accept(l, Duration::from_secs(1)).unwrap();
    set_busy_poll(fd, Duration::ZERO).unwrap();
    assert_eq!(write(fd, b"hello").unwrap(), 5);
    wait_readable(sfd);
    let mut b = [0u8; 16];
    assert_eq!(read(sfd, &mut b).unwrap(), 5);
    assert_eq!(&b[..5], b"hello");
    let iov = [
        libc::iovec {
            iov_base: b"ab".as_ptr() as *mut _,
            iov_len: 2,
        },
        libc::iovec {
            iov_base: b"cde".as_ptr() as *mut _,
            iov_len: 3,
        },
    ];
    assert_eq!(writev(fd, &iov).unwrap(), 5);
    wait_readable(sfd);
    assert_eq!(read(sfd, &mut b).unwrap(), 5);
    assert_eq!(&b[..5], b"abcde");
    assert_eq!(
        read(sfd, &mut b).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    shutdown_write(fd).unwrap();
    wait_readable(sfd);
    assert_eq!(read(sfd, &mut b).unwrap(), 0);
    close(fd);
    close(sfd);
    close(l);
}

#[test]
fn connect_refused() {
    let (l, la) = listen_tcp(lo4(), false, 0).unwrap();
    close(l);
    let (fd, in_progress) = match connect(la, None) {
        Ok(v) => v,
        Err(e) => {
            assert_eq!(e.raw_os_error(), Some(libc::ECONNREFUSED));
            return;
        }
    };
    assert!(in_progress);
    wait_writable(fd);
    let e = connect_error(fd).unwrap_err();
    assert_eq!(e.raw_os_error(), Some(libc::ECONNREFUSED));
    close(fd);
}

#[test]
fn connect_with_local_bind() {
    let (l, la) = listen_tcp(lo4(), false, 0).unwrap();
    let (fd, _) = connect(la, Some(lo4())).unwrap();
    wait_writable(fd);
    connect_error(fd).unwrap();
    assert!(local_addr(fd).unwrap().port() != 0);
    close(fd);
    close(l);
}

#[test]
fn udp_batch_round_trip() {
    let (rx, ra) = listen_udp(lo4(), false).unwrap();
    let mut sb = Batch::new(10, 64);
    assert_eq!(sb.len(), 10);
    for i in 0..10 {
        let msg = format!("datagram-{i}");
        sb.buf_mut(i)[..msg.len()].copy_from_slice(msg.as_bytes());
        sb.set_len(i, msg.len());
        sb.set_addr(i, ra);
    }
    let (sfd, _) = listen_udp(lo4(), false).unwrap();
    assert_eq!(sb.send_mmsg(sfd, 10).unwrap(), 10);
    let sfd_addr = local_addr(sfd).unwrap();
    let mut rb = Batch::new(16, 64);
    wait_readable(rx);
    let mut got = 0;
    while got < 10 {
        let n = rb.recv_mmsg(rx).unwrap();
        for i in 0..n {
            let (p, a) = rb.msg(i);
            assert_eq!(p, format!("datagram-{}", got + i).as_bytes());
            assert_eq!(a, Some(sfd_addr));
        }
        got += n;
    }
    assert_eq!(got, 10);
    assert_eq!(
        rb.recv_mmsg(rx).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    // portable single-datagram helpers
    assert_eq!(send_to(sfd, b"one", ra).unwrap(), 3);
    wait_readable(rx);
    let mut b = [0u8; 8];
    let (n, from) = recv_from(rx, &mut b).unwrap();
    assert_eq!((&b[..n], from), (&b"one"[..], sfd_addr));
    close(sfd);
    close(rx);
}

#[test]
fn ipv6_loopback_round_trip() {
    let a6: SocketAddr = "[::1]:0".parse().unwrap();
    let (l, la) = match listen_tcp(a6, false, 0) {
        Ok(v) => v,
        Err(_) => return, // IPv6 unavailable
    };
    assert!(la.is_ipv6());
    let (fd, _) = connect(la, None).unwrap();
    wait_writable(fd);
    connect_error(fd).unwrap();
    let (sfd, peer) = accept(l).unwrap();
    assert!(peer.is_ipv6());
    assert_eq!(write(fd, b"v6").unwrap(), 2);
    wait_readable(sfd);
    let mut b = [0u8; 4];
    assert_eq!(read(sfd, &mut b).unwrap(), 2);
    close(fd);
    close(sfd);
    close(l);

    let (u, ua) = listen_udp(a6, false).unwrap();
    let (u2, u2a) = listen_udp(a6, false).unwrap();
    let mut sb = Batch::new(2, 16);
    sb.buf_mut(0)[..2].copy_from_slice(b"hi");
    sb.set_len(0, 2);
    sb.set_addr(0, ua);
    assert_eq!(sb.send_mmsg(u2, 1).unwrap(), 1);
    wait_readable(u);
    let mut rb = Batch::new(2, 16);
    assert_eq!(rb.recv_mmsg(u).unwrap(), 1);
    assert_eq!(rb.msg(0), (&b"hi"[..], Some(u2a)));
    close(u);
    close(u2);
}

#[test]
fn sockaddr_round_trip() {
    // SAFETY: all-zero sockaddr_storage is valid.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let v4: SocketAddr = "10.1.2.3:4567".parse().unwrap();
    let l = to_sockaddr(v4, &mut ss);
    assert_eq!(l as usize, size_of::<libc::sockaddr_in>());
    assert_eq!(from_sockaddr(&ss, l), Some(v4));
    let v6 = SocketAddr::V6(SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        9000,
        0x12345,
        7,
    ));
    let l = to_sockaddr(v6, &mut ss);
    assert_eq!(l as usize, size_of::<libc::sockaddr_in6>());
    assert_eq!(from_sockaddr(&ss, l), Some(v6));
    // truncated lengths are rejected
    assert_eq!(from_sockaddr(&ss, 4), None);
    assert_eq!(
        from_sockaddr(&ss, (size_of::<libc::sockaddr_in6>() - 1) as socklen_t),
        None
    );
    // unknown family
    ss.ss_family = libc::AF_UNIX as libc::sa_family_t;
    assert_eq!(from_sockaddr(&ss, 110), None);
}

#[test]
fn hot_paths_do_not_allocate() {
    let (l, la) = listen_tcp(lo4(), false, 0).unwrap();
    let (c, _) = connect(la, None).unwrap();
    wait_writable(c);
    let (s, _) = accept(l).unwrap(); // warm up
    close(s);
    close(c);
    let c2 = TcpStream::connect(la).unwrap();
    let mut got = None;
    let n = twine_alloccount::allocs(|| got = Some(accept(l).unwrap()));
    assert_eq!(n, 0, "accept allocates");
    let (s, _) = got.unwrap();
    drop(c2);
    close(s);

    let (c, _) = connect(la, None).unwrap();
    wait_writable(c);
    let (s, _) = accept(l).unwrap();
    let data = [7u8; 100];
    let iov = [libc::iovec {
        iov_base: data.as_ptr() as *mut _,
        iov_len: 100,
    }];
    let mut sink = [0u8; 512];
    let w = twine_alloccount::allocs(|| {
        write(c, &data).unwrap();
        writev(c, &iov).unwrap();
    });
    assert_eq!(w, 0, "write/writev allocate");
    let mut total = 0;
    let r = twine_alloccount::allocs(|| {
        while total < 200 {
            match read(s, &mut sink) {
                Ok(n) => total += n,
                Err(e) => assert_eq!(e.kind(), io::ErrorKind::WouldBlock),
            }
        }
    });
    assert_eq!(r, 0, "read allocates");

    let v6 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 2, 3));
    // SAFETY: all-zero sockaddr_storage is valid.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let a = twine_alloccount::allocs(|| {
        let l = to_sockaddr(v6, &mut ss);
        assert_eq!(from_sockaddr(&ss, l), Some(v6));
    });
    assert_eq!(a, 0, "sockaddr conversion allocates");

    let (rx, ra) = listen_udp(lo4(), false).unwrap();
    let (tx, _) = listen_udp(lo4(), false).unwrap();
    let mut sb = Batch::new(4, 32);
    let mut rb = Batch::new(4, 32);
    for i in 0..4 {
        sb.set_len(i, 8);
        sb.set_addr(i, ra);
    }
    let m = twine_alloccount::allocs(|| {
        assert_eq!(sb.send_mmsg(tx, 4).unwrap(), 4);
    });
    assert_eq!(m, 0, "send_mmsg allocates");
    wait_readable(rx);
    let m = twine_alloccount::allocs(|| {
        assert_eq!(rb.recv_mmsg(rx).unwrap(), 4);
    });
    assert_eq!(m, 0, "recv_mmsg allocates");
    close(rx);
    close(tx);
    close(c);
    close(s);
    close(l);
}
