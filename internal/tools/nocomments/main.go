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
	"regexp"
	"slices"
	"strings"
)

var (
	hashComment  = regexp.MustCompile(`^\s*#`)
	slashComment = regexp.MustCompile(`^\s*(//|/\*|\{\s*/\*)`)
	hashTypes    = []string{".yml", ".yaml", ".gitignore", ".dockerignore", ".gitattributes", ".prettierignore"}
	slashTypes   = []string{".ts", ".tsx", ".js", ".mjs", ".proto"}
	markupTypes  = []string{".svg", ".md", ".html"}
	plainTypes   = []string{".json", ".sum", ".txt"}
)

type finder func(path string, src []byte) ([]int, error)

func parseGo(path string, src []byte) (*token.FileSet, *ast.File, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	return fset, f, err
}

func strip(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fset, f, err := parseGo(path, src)
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

func finderFor(name string) (finder, error) {
	ext := filepath.Ext(name)
	switch {
	case ext == ".go":
		return goComments, nil
	case ext == ".sh":
		return byLine(hashComment, true), nil
	case strings.Contains(strings.ToLower(name), "dockerfile"), slices.Contains(hashTypes, ext):
		return byLine(hashComment, false), nil
	case slices.Contains(slashTypes, ext), name == "go.mod", name == "tsconfig.json":
		return byLine(slashComment, false), nil
	case ext == ".css":
		return anywhere("/*"), nil
	case slices.Contains(markupTypes, ext):
		return anywhere("<!--"), nil
	case slices.Contains(plainTypes, ext):
		return nil, nil
	}
	return nil, fmt.Errorf("%s: no comment rule for this kind of file", name)
}

func goComments(path string, src []byte) ([]int, error) {
	fset, f, err := parseGo(path, src)
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

func textLines(src []byte) []string {
	return strings.Split(string(bytes.TrimPrefix(src, []byte{0xef, 0xbb, 0xbf})), "\n")
}

func byLine(comment *regexp.Regexp, shebang bool) finder {
	return func(_ string, src []byte) ([]int, error) {
		var lines []int
		for i, line := range textLines(src) {
			if comment.MatchString(line) && !(shebang && i == 0 && strings.HasPrefix(line, "#!")) {
				lines = append(lines, i+1)
			}
		}
		return lines, nil
	}
}

func anywhere(marker string) finder {
	return func(_ string, src []byte) ([]int, error) {
		var lines []int
		for i, line := range textLines(src) {
			if strings.Contains(line, marker) {
				lines = append(lines, i+1)
			}
		}
		return lines, nil
	}
}

func comments(path string) ([]int, error) {
	find, err := finderFor(filepath.Base(path))
	if err != nil || find == nil {
		return nil, err
	}
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return find(path, src)
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
		flag.PrintDefaults()
	}
	flag.Parse()
	if *checkOnly && flag.NArg() > 0 {
		if check(flag.Args()) > 0 {
			os.Exit(1)
		}
		return
	}
	if *checkOnly || flag.NArg() != 1 {
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
