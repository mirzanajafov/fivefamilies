package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func strip(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return err
	}
	f.Comments = nil
	f.Doc = nil
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			x.Doc = nil
		case *ast.GenDecl:
			x.Doc = nil
		case *ast.ValueSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.TypeSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.ImportSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.Field:
			x.Doc, x.Comment = nil, nil
		}
		return true
	})
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, f); err != nil {
		return err
	}
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

func comments(path string) ([]int, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	switch filepath.Ext(name) {
	case ".go":
		return goComments(path, src)
	case ".ts", ".tsx", ".mjs", ".css", ".proto":
		return linesStarting(src, "//", "/*", "{/*"), nil
	case ".svg":
		return linesStarting(src, "<!--"), nil
	case ".yml", ".yaml", ".sh", ".gitignore", ".dockerignore", ".gitattributes":
		return hashComments(src), nil
	}
	if strings.HasPrefix(name, "Dockerfile") {
		return hashComments(src), nil
	}
	return nil, nil
}

func hashComments(src []byte) []int {
	lines := linesStarting(src, "#")
	if len(lines) > 0 && lines[0] == 1 && bytes.HasPrefix(src, []byte("#!")) {
		return lines[1:]
	}
	return lines
}

func goComments(path string, src []byte) ([]int, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var lines []int
	for _, g := range f.Comments {
		for _, c := range g.List {
			lines = append(lines, fset.Position(c.Pos()).Line)
		}
	}
	return lines, nil
}

func linesStarting(src []byte, prefixes ...string) []int {
	var lines []int
	for i, line := range strings.Split(string(src), "\n") {
		trimmed := strings.TrimSpace(line)
		if slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(trimmed, p) }) {
			lines = append(lines, i+1)
		}
	}
	return lines
}

func check(paths []string) int {
	found := 0
	for _, path := range paths {
		lines, err := comments(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			found++
		}
		for _, line := range lines {
			fmt.Fprintf(os.Stderr, "%s:%d: a comment\n", path, line)
		}
		found += len(lines)
	}
	return found
}

func main() {
	checkOnly := flag.Bool("check", false, "report comments in the files given instead of stripping a directory of Go files")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: nocomments <dir> | nocomments -check <file>...")
	}
	flag.Parse()
	if *checkOnly {
		if check(flag.Args()) > 0 {
			os.Exit(1)
		}
		return
	}
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	err := filepath.WalkDir(flag.Arg(0), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		return strip(path)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
