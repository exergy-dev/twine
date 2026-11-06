//go:build darwin || dragonfly || freebsd || openbsd

package poll

import "golang.org/x/sys/unix"

// evReceipt makes kevent return a receipt for every change, so Mod and Del
// submit all changes in one syscall without consuming pending events.
const evReceipt = unix.EV_RECEIPT
