package twine_test

import (
	"os"
	"strings"
	"testing"
)

// TestReadmeSnippetsCompile checks that every Go code block in README.md is
// a verbatim copy of text in one of the example files, so the README cannot
// drift from code that compiles and (where it has an Output) runs. Import
// blocks are skipped: they only show the package aliases.
func TestReadmeSnippetsCompile(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	var sources []string
	for _, f := range []string{
		"example_echo_test.go", "example_test.go",
		"http/example_test.go", "tls/example_test.go",
	} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, string(b))
	}
	rest := string(readme)
	n := 0
	for {
		_, after, ok := strings.Cut(rest, "```go\n")
		if !ok {
			break
		}
		block, after, ok := strings.Cut(after, "```")
		if !ok {
			t.Fatal("unterminated ```go block in README.md")
		}
		rest = after
		if strings.HasPrefix(block, "import (") {
			continue
		}
		// A complete program says "package main"; the example file that
		// holds it is in a _test package. The rest must match.
		if strings.HasPrefix(block, "package ") {
			_, block, _ = strings.Cut(block, "\n")
		}
		n++
		found := false
		for _, src := range sources {
			if strings.Contains(src, block) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("README.md code block %d is not a verbatim copy of any example file:\n%s", n, block)
		}
	}
	if n < 6 {
		t.Errorf("found %d Go code blocks in README.md, want the echo server and the five snippets", n)
	}
}
