package main

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	hashComment     = regexp.MustCompile(`^\s*#`)
	slashComment    = regexp.MustCompile(`^\s*(//|/\*)|\{\s*/\*`)
	markupComment   = regexp.MustCompile(`<!--`)
	markdownComment = regexp.MustCompile(`<!--|^\s*\[//\]:`)
	cssString       = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	hashTypes       = []string{".yml", ".yaml", ".gitignore", ".dockerignore", ".gitattributes", ".prettierignore"}
	slashTypes      = []string{".ts", ".tsx", ".mjs", ".proto", ".json", ".mod"}
	pngSignature    = []byte("\x89PNG\r\n\x1a\n")
	pngImageChunks  = []string{"IHDR", "PLTE", "IDAT", "IEND"}
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

func isDockerfile(name string) bool {
	lower := strings.ToLower(name)
	return lower == "dockerfile" || strings.HasPrefix(lower, "dockerfile.")
}

func finderFor(path string) (finder, error) {
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	switch {
	case ext == ".go":
		return goComments, nil
	case ext == ".sh":
		return shellComments, nil
	case slices.Contains(hashTypes, ext), isDockerfile(name):
		return byLine(hashComment.MatchString), nil
	case slices.Contains(slashTypes, ext):
		return byLine(slashComment.MatchString), nil
	case ext == ".css":
		return byLine(func(line string) bool { return strings.Contains(cssString.ReplaceAllString(line, ""), "/*") }), nil
	case ext == ".svg":
		return byLine(markupComment.MatchString), nil
	case ext == ".md":
		return byLine(markdownComment.MatchString), nil
	case ext == ".png":
		return pngComments, nil
	case ext == ".sum", ext == ".txt" && strings.HasPrefix(filepath.ToSlash(filepath.Clean(path)), "bench/results/profile/"):
		return skip, nil
	}
	return nil, errors.New("no comment rule for this kind of file")
}

func skip(string, []byte) ([]int, error) {
	return nil, nil
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

func byLine(isComment func(string) bool) finder {
	return func(_ string, src []byte) ([]int, error) {
		var lines []int
		for i, line := range strings.Split(string(bytes.TrimPrefix(src, []byte{0xef, 0xbb, 0xbf})), "\n") {
			if isComment(line) {
				lines = append(lines, i+1)
			}
		}
		return lines, nil
	}
}

func pngComments(path string, src []byte) ([]int, error) {
	if !bytes.HasPrefix(src, pngSignature) {
		return nil, fmt.Errorf("%s: not a PNG", path)
	}
	for i, n := len(pngSignature), 1; ; n++ {
		if len(src)-i < 12 {
			return nil, fmt.Errorf("%s: chunk %d is cut off before the image end", path, n)
		}
		size, kind := binary.BigEndian.Uint32(src[i:]), string(src[i+4:i+8])
		if uint64(size) > uint64(len(src)-i-12) {
			return nil, fmt.Errorf("%s: chunk %d (%s) runs past the end of the file", path, n, kind)
		}
		if !slices.Contains(pngImageChunks, kind) {
			return nil, fmt.Errorf("%s: chunk %d is %s, and a PNG may hold only %v, since the others can carry text", path, n, kind, pngImageChunks)
		}
		i += 12 + int(size)
		if kind == "IEND" {
			if i < len(src) {
				return nil, fmt.Errorf("%s: %d bytes after the image end", path, len(src)-i)
			}
			return nil, nil
		}
	}
}

func shellComments(path string, src []byte) ([]int, error) {
	lines, err := byLine(hashComment.MatchString)(path, src)
	if len(lines) > 0 && lines[0] == 1 && bytes.HasPrefix(src, []byte("#!")) {
		lines = lines[1:]
	}
	return lines, err
}

func comments(path string) ([]int, error) {
	find, err := finderFor(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
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

func stripDir(dir string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		return strip(path)
	})
}

func run(args []string) int {
	flags := flag.NewFlagSet("nocomments", flag.ContinueOnError)
	checkOnly := flags.Bool("check", false, "report comments in the files given instead of stripping a directory of Go files")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(flags.Output(), "usage: nocomments <dir> | nocomments -check <file>...")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	switch {
	case *checkOnly && flags.NArg() > 0:
		if check(flags.Args()) > 0 {
			return 1
		}
		return 0
	case !*checkOnly && flags.NArg() == 1:
		if err := stripDir(flags.Arg(0)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	flags.Usage()
	return 2
}

func main() {
	os.Exit(run(os.Args[1:]))
}
