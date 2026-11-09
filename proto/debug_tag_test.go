//go:build !twine_debug

package proto

// debugBuild reports a -tags twine_debug build, whose core assertions add
// allocations that the AMP allocation gate does not budget for.
const debugBuild = false
