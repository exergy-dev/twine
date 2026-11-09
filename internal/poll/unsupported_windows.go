//go:build windows

package poll

// Twine needs epoll (Linux) or kqueue (macOS and the BSDs). Windows is not
// supported; this undefined name makes a Windows build say so.
var _ = twine_does_not_support_windows
