//go:build linux

package twtls

import (
	"crypto/tls"
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

// loopbackConn returns one end of a connected loopback TCP pair and a
// function that closes both ends.
func loopbackConn() (fd int, closeFn func(), err error) {
	lfd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, nil, err
	}
	fail := func(e error, fds ...int) (int, func(), error) {
		for _, f := range fds {
			unix.Close(f)
		}
		return -1, nil, e
	}
	if err := unix.Bind(lfd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		return fail(err, lfd)
	}
	if err := unix.Listen(lfd, 1); err != nil {
		return fail(err, lfd)
	}
	sa, err := unix.Getsockname(lfd)
	if err != nil {
		return fail(err, lfd)
	}
	cfd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fail(err, lfd)
	}
	// A blocking connect to a listening loopback socket completes from the
	// backlog without an accept.
	if err := unix.Connect(cfd, sa); err != nil {
		return fail(err, lfd, cfd)
	}
	return cfd, func() { unix.Close(cfd); unix.Close(lfd) }, nil
}

// probeKTLS reports whether the kernel accepts the "tls" ULP. The ULP can
// only be attached to an established socket, so the probe connects a
// throwaway loopback pair. If that cannot be set up it falls back to an
// unconnected socket, where ENOTCONN means the ULP exists (tls_init rejects
// non-established sockets) and ENOENT means it does not.
func probeKTLS() bool {
	if fd, closeFn, err := loopbackConn(); err == nil {
		defer closeFn()
		return unix.SetsockoptString(fd, unix.SOL_TCP, unix.TCP_ULP, "tls") == nil
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	err = unix.SetsockoptString(fd, unix.SOL_TCP, unix.TCP_ULP, "tls")
	return err == nil || errors.Is(err, unix.ENOTCONN)
}

// warmKTLS runs the capability probes (KTLSAvailable and the per-cipher key
// probes) now, so that they do not run on a loop thread at the first
// handshake, where the sync.Once would also block every other loop
// finishing a handshake at the same time. Called when a factory is built
// with KernelOffload(true); later calls return at once.
func warmKTLS() {
	if KTLSAvailable() {
		for k := ktlsKind(0); k < numKinds; k++ {
			cipherUsable(k)
		}
	}
}

// setCryptoInfo calls setsockopt(SOL_TLS, opt, info). unix has no helper for
// an arbitrary byte buffer at an arbitrary level.
func setCryptoInfo(fd, opt int, info []byte) error {
	_, _, e := unix.Syscall6(unix.SYS_SETSOCKOPT, uintptr(fd), unix.SOL_TLS, uintptr(opt),
		uintptr(unsafe.Pointer(&info[0])), uintptr(len(info)), 0)
	if e != 0 {
		return e
	}
	return nil
}

// probeCipher checks, on a throwaway socket, that the kernel accepts both
// TLS_TX and TLS_RX with info (dummy keys).
func probeCipher(info []byte) bool {
	fd, closeFn, err := loopbackConn()
	if err != nil {
		return false
	}
	defer closeFn()
	if unix.SetsockoptString(fd, unix.SOL_TCP, unix.TCP_ULP, "tls") != nil {
		return false
	}
	return setCryptoInfo(fd, tlsTX, info) == nil && setCryptoInfo(fd, tlsRX, info) == nil
}

// installKTLS moves record protection into the kernel after the handshake
// and reports whether it did. It runs on the loop with the bio in loop mode.
// Any precondition that does not hold, and any unexpected state, leaves the
// connection on the memory BIO (false). See the package documentation for
// the rules (server: TX+RX when no input is buffered, else TX only; client:
// TX only).
func installKTLS(t *Transport) bool {
	if !KTLSAvailable() {
		return false
	}
	fder, ok := t.under.(interface{ FD() int })
	if !ok {
		return false
	}
	fd := fder.FD()
	if fd < 0 {
		return false
	}
	st := t.tc.ConnectionState()
	if st.Version != tls.VersionTLS13 {
		return false
	}
	kind, keyLen, h, ok := kindFor(st.CipherSuite)
	if !ok || !cipherUsable(kind) {
		return false
	}
	// Ciphertext still queued for the socket would be encrypted a second
	// time (or reordered against the first kernel record). The last
	// handshake flight has already been handed to the core (flushOut before
	// the mode switch), so the core's ring is the only place it can wait.
	if t.under.PendingWrite() != 0 {
		return false
	}
	snap, ok := snapshot(t.tc, st.CipherSuite, tls.VersionTLS13)
	if !ok || !seqSane(snap.out.seq) || !seqSane(snap.in.seq) ||
		len(snap.out.secret) != h().Size() || len(snap.in.secret) != h().Size() {
		return false
	}
	tx, err := deriveKeys(h, snap.out.secret, keyLen, snap.out.seq)
	if err != nil {
		return false
	}
	// RX: server only (a client would receive NewSessionTicket records,
	// which the kernel hands out only through recvmsg control messages), and
	// only if nothing from the peer has been consumed or is queued.
	var rx *ktlsKeys
	if !t.f.client && snap.cleanInput && !t.bio.hasInbound() {
		if rx, err = deriveKeys(h, snap.in.secret, keyLen, snap.in.seq); err != nil {
			rx = nil
		}
	}

	if err := unix.SetsockoptString(fd, unix.SOL_TCP, unix.TCP_ULP, "tls"); err != nil && !errors.Is(err, unix.EEXIST) {
		return false
	}
	if err := setCryptoInfo(fd, tlsTX, tx.cryptoInfo(kind)); err != nil {
		return false // nothing committed: the ULP without keys is a passthrough
	}
	t.bio.blockWrites() // from here crypto/tls must never write
	if rx != nil {
		if err := setCryptoInfo(fd, tlsRX, rx.cryptoInfo(kind)); err == nil {
			t.rxOffloaded = true
		}
		// An RX failure after TX cannot undo TX; RX stays on the memory BIO.
	}
	return true
}

// sendCloseNotify sends a close_notify alert as a kernel-encrypted TLS record
// on fd, using a TLS_SET_RECORD_TYPE control message. The kernel must have
// TX keys installed. It never blocks (MSG_DONTWAIT) and returns EAGAIN when
// the send buffer is full.
func sendCloseNotify(fd int) error {
	oob := make([]byte, unix.CmsgSpace(1))
	hdr := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	hdr.Level = unix.SOL_TLS
	hdr.Type = tlsSetRecordType
	hdr.SetLen(unix.CmsgLen(1))
	oob[unix.CmsgSpace(0)] = recordTypeAlert
	return unix.Sendmsg(fd, []byte{1, 0}, oob, nil, unix.MSG_DONTWAIT) // warning, close_notify
}

// hasInbound reports whether received ciphertext is queued or borrowed.
// Loop-only, loop mode.
func (b *bio) hasInbound() bool { return b.r < len(b.in) || len(b.borrowed) > 0 }

// blockWrites makes every later Write fail with errOffloadWrite. Loop-only.
func (b *bio) blockWrites() {
	b.mu.Lock()
	b.noWrite = true
	b.mu.Unlock()
}
