package raft

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCoreStaysDeterministic(t *testing.T) {
	allowed := map[string]bool{
		"cmp":             true,
		"encoding/binary": true,
		"errors":          true,
		"fmt":             true,
		"slices":          true,
		"strings":         true,
		"github.com/mirzanajafov/fivefamilies/internal/planted": true,
	}
	for _, dir := range []string{".", "../planted"} {
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			name := filepath.Join(dir, f.Name())
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range parsed.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if !allowed[path] {
					t.Errorf("%s imports %s, but the core may not use clocks, goroutines, I/O or global randomness", name, path)
				}
			}
		}
	}
}
