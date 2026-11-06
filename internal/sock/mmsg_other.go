//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package sock

import "golang.org/x/sys/unix"

// Batch is a reusable set of datagram slots. Without recvmmsg/sendmmsg it
// loops over recvfrom/sendto with the same observable behaviour as on Linux.
// A Batch is not safe for concurrent use.
type Batch struct {
	msgs []Message
	// Reusable destination storage for sendto.
	sa4 unix.SockaddrInet4
	sa6 unix.SockaddrInet6
}

func (b *Batch) init() {}

// RecvMmsg receives up to len(Msgs()) datagrams (one recvfrom(2) each until
// EAGAIN), setting N and Addr/AddrLen on each received message. It returns
// the number received, or unix.EAGAIN when nothing is queued. EINTR is
// retried internally. Each received datagram allocates one unix.Sockaddr
// (golang.org/x/sys Recvfrom); the EAGAIN path does not allocate.
func (b *Batch) RecvMmsg(fd int) (int, error) { return b.recvLoop(fd) }

// SendMmsg sends the first n messages (one sendto(2) each), setting N on each
// sent message. It returns the number sent, which may be fewer than n; the
// error (e.g. unix.EAGAIN) is returned only when none was sent. EINTR is
// retried internally. It does not allocate.
func (b *Batch) SendMmsg(fd int, n int) (int, error) { return b.sendLoop(fd, n) }
