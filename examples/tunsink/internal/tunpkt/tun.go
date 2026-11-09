//go:build linux

// Package tunpkt holds the TUN helpers and packet builder shared by the
// tunsink server, the baseline server and the load generator.
package tunpkt

import (
	"encoding/binary"

	"golang.org/x/sys/unix"
)

// OpenTUN attaches to an existing TUN interface and returns a non-blocking
// descriptor delivering raw IP packets (IFF_TUN | IFF_NO_PI).
func OpenTUN(name string) (int, error) {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	ifr, err := unix.NewIfreq(name)
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		unix.Close(fd)
		return -1, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// IPv4UDP builds a valid IPv4/UDP packet of exactly size bytes (min 28)
// from src to dst with a zero-filled payload. The UDP checksum is left 0
// (permitted for IPv4).
func IPv4UDP(src, dst [4]byte, srcPort, dstPort uint16, size int) []byte {
	if size < 28 {
		size = 28
	}
	p := make([]byte, size)
	p[0] = 0x45 // v4, IHL 5
	binary.BigEndian.PutUint16(p[2:], uint16(size))
	p[6] = 0x40 // DF
	p[8] = 64   // TTL
	p[9] = 17   // UDP
	copy(p[12:], src[:])
	copy(p[16:], dst[:])
	binary.BigEndian.PutUint16(p[10:], ipChecksum(p[:20]))
	binary.BigEndian.PutUint16(p[20:], srcPort)
	binary.BigEndian.PutUint16(p[22:], dstPort)
	binary.BigEndian.PutUint16(p[24:], uint16(size-20))
	return p
}

func ipChecksum(h []byte) uint16 {
	var s uint32
	for i := 0; i+1 < len(h); i += 2 {
		s += uint32(binary.BigEndian.Uint16(h[i:]))
	}
	for s>>16 != 0 {
		s = s&0xffff + s>>16
	}
	return ^uint16(s)
}
