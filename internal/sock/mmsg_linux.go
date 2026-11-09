package sock

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmsghdr mirrors struct mmsghdr. golang.org/x/sys v0.30.0 has no Mmsghdr
// type; Go's struct layout rounds the size up to Msghdr's alignment, which
// matches the C layout on both 32- and 64-bit targets.
type mmsghdr struct {
	Hdr unix.Msghdr
	Len uint32
}

// Batch is a reusable set of datagram slots for recvmmsg/sendmmsg. All
// kernel-facing arrays are preallocated by NewBatch. A Batch is not safe for
// concurrent use.
type Batch struct {
	msgs []Message
	hdrs []mmsghdr
	iovs []unix.Iovec
	// Reusable storage for the portable sendto/recvfrom loop.
	sa4 unix.SockaddrInet4
	sa6 unix.SockaddrInet6
}

func (b *Batch) init() {
	b.hdrs = make([]mmsghdr, len(b.msgs))
	b.iovs = make([]unix.Iovec, len(b.msgs))
	for i := range b.hdrs {
		b.hdrs[i].Hdr.Iov = &b.iovs[i]
		b.hdrs[i].Hdr.SetIovlen(1)
	}
}

// setIov points iov at buf.
func setIov(iov *unix.Iovec, buf []byte) {
	if len(buf) > 0 {
		iov.Base = &buf[0]
	} else {
		iov.Base = nil
	}
	iov.SetLen(len(buf))
}

// RecvMmsg receives up to len(Msgs()) datagrams with one recvmmsg(2) call,
// setting N and Addr/AddrLen on each received message. It returns the number
// of messages received, or unix.EAGAIN when nothing is queued. EINTR is
// retried internally. It does not allocate.
func (b *Batch) RecvMmsg(fd int) (int, error) {
	for i := range b.msgs {
		m := &b.msgs[i]
		h := &b.hdrs[i]
		setIov(&b.iovs[i], m.Buf)
		h.Hdr.Name = (*byte)(unsafe.Pointer(&m.Addr))
		h.Hdr.Namelen = unix.SizeofSockaddrInet6
		h.Hdr.Flags = 0
		h.Len = 0
	}
	for {
		r, _, e := unix.Syscall6(unix.SYS_RECVMMSG, uintptr(fd),
			uintptr(unsafe.Pointer(&b.hdrs[0])), uintptr(len(b.hdrs)),
			unix.MSG_DONTWAIT, 0, 0)
		if e == unix.EINTR {
			continue
		}
		if e != 0 {
			return 0, e
		}
		n := int(r)
		for i := 0; i < n; i++ {
			b.msgs[i].N = int(b.hdrs[i].Len)
			b.msgs[i].AddrLen = b.hdrs[i].Hdr.Namelen
		}
		return n, nil
	}
}

// SendMmsg sends the first n messages (capped at len(Msgs())) with one
// sendmmsg(2) call, to each message's Addr (or the connected peer when
// AddrLen is 0), setting N on each sent message. It returns the number of
// messages sent, which may be fewer than n; the error (e.g. unix.EAGAIN) is
// returned only when none was sent. EINTR is retried internally. It does not
// allocate.
func (b *Batch) SendMmsg(fd int, n int) (int, error) {
	if n > len(b.msgs) {
		n = len(b.msgs)
	}
	if n <= 0 {
		return 0, nil
	}
	for i := 0; i < n; i++ {
		m := &b.msgs[i]
		h := &b.hdrs[i]
		setIov(&b.iovs[i], m.Buf)
		if m.AddrLen > 0 {
			h.Hdr.Name = (*byte)(unsafe.Pointer(&m.Addr))
			h.Hdr.Namelen = m.AddrLen
		} else {
			h.Hdr.Name = nil
			h.Hdr.Namelen = 0
		}
		h.Hdr.Flags = 0
		h.Len = 0
	}
	for {
		r, _, e := unix.Syscall6(unix.SYS_SENDMMSG, uintptr(fd),
			uintptr(unsafe.Pointer(&b.hdrs[0])), uintptr(n),
			unix.MSG_DONTWAIT, 0, 0)
		if e == unix.EINTR {
			continue
		}
		if e != 0 {
			return 0, e
		}
		sent := int(r)
		for i := 0; i < sent; i++ {
			b.msgs[i].N = int(b.hdrs[i].Len)
		}
		return sent, nil
	}
}
