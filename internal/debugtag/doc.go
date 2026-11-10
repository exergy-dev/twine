// Package debugtag reports whether the binary was built with the
// twine_debug build tag. The internal packages guard their invariant
// checks with
//
//	if debugtag.On { ... }
//
// so a release build compiles them out entirely.
package debugtag
