package poll

// evReceipt is zero because golang.org/x/sys does not define EV_RECEIPT for
// NetBSD; Mod and Del then submit one change per kevent call.
const evReceipt = 0
