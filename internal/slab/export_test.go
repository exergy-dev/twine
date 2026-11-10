package slab

// ClassFor returns the class size for n or -1 if n is larger than MaxClass.
// n <= 0 maps to the smallest class. Test helper.
func ClassFor(n int) int {
	i := classIndex(n)
	if i < 0 {
		return -1
	}
	return classes[i]
}
