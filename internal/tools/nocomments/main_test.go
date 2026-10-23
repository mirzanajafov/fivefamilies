package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func write(t *testing.T, path, src string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func png(kinds ...string) string {
	src := "\x89PNG\r\n\x1a\n"
	for _, kind := range kinds {
		src += "\x00\x00\x00\x03" + kind + "why" + "crc!"
	}
	return src
}

func TestCommentsAreFoundInEveryKindOfFileTheRepoHolds(t *testing.T) {
	t.Chdir(t.TempDir())
	cases := map[string]struct {
		src  string
		want []int
	}{
		"a.go":                              {"package a\n\n// why\nfunc f() {}\n\nvar x = 1 /* why */\n", []int{3, 6}},
		"b.go":                              {"package b\n\nvar url = \"http://example.com\"\n", nil},
		"c.ts":                              {"const a = 'http://example.com'\n// why\nconst b = 1\n", []int{2}},
		"d.tsx":                             {"<div>\n  {/* why */}\n  { /* why */ }\n  <p>{/* why */}</p>\n</div>\n", []int{2, 3, 4}},
		"e.mjs":                             {"\xef\xbb\xbf// why\nexport default {}\n", []int{1}},
		"f.proto":                           {"syntax = \"proto3\";\n// why\n", []int{2}},
		"g.css":                             {"a { content: \"/*\"; }\nb { color: red; } /* why */\n", []int{2}},
		"h.yml":                             {"a: 1\n# why\nb: '#not-a-comment'\n", []int{2}},
		"i.yaml":                            {"#! why\n", []int{1}},
		"j.sh":                              {"#!/bin/sh\necho '#'\n  # why\n", []int{3}},
		"k.sh":                              {"# why\necho\n", []int{1}},
		"Dockerfile":                        {"FROM scratch\n# why\n", []int{2}},
		"lab/Dockerfile.net":                {"#! why\nFROM scratch\n", []int{1}},
		".gitignore":                        {"node_modules\n# why\n", []int{2}},
		".dockerignore":                     {"# why\n", []int{1}},
		".gitattributes":                    {"# why\n", []int{1}},
		".prettierignore":                   {"# why\n", []int{1}},
		"go.mod":                            {"module x\n\n// why\n", []int{3}},
		"tsconfig.json":                     {"{\n  // why\n}\n", []int{2}},
		"l.json":                            {"{\"a\": \"// not a comment\"}\n", nil},
		"m.svg":                             {"<svg><!-- why --><path/></svg>\n", []int{1}},
		"README.md":                         {"text\n[//]: # (why)\n<!-- why -->\n", []int{2, 3}},
		"go.sum":                            {"example.com v1.0.0 h1:x\n", nil},
		"./bench/results/profile/a/cpu.txt": {"# not a comment in profile output\n", nil},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := comments(write(t, name, c.src))
			if err != nil || !slices.Equal(got, c.want) {
				t.Errorf("comments on lines %v, want %v: %v", got, c.want, err)
			}
		})
	}
}

func TestAFileWithNoCommentRuleFailsTheCheck(t *testing.T) {
	t.Chdir(t.TempDir())
	clean := write(t, "a.go", "package a\n")
	if n := check([]string{clean}); n != 0 {
		t.Fatalf("a clean file counted %d problems", n)
	}
	for _, name := range []string{"schema.sql", "notes.txt", "bench/results/notes.txt", "web.dockerfile", "dockerfile-notes"} {
		if n := check([]string{clean, write(t, name, "-- why\n")}); n != 1 {
			t.Errorf("%s: counted %d problems, want 1", name, n)
		}
	}
}

func TestAPNGMayHoldOnlyImageData(t *testing.T) {
	t.Chdir(t.TempDir())
	if lines, err := comments(write(t, "image.png", png("IHDR", "PLTE", "IDAT", "IDAT", "IEND"))); lines != nil || err != nil {
		t.Fatalf("an image with nothing else: %v, %v", lines, err)
	}
	refused := map[string]string{
		"text.png":         png("IHDR", "tEXt", "IDAT", "IEND"),
		"zipped.png":       png("IHDR", "IDAT", "zTXt", "IEND"),
		"unicode.png":      png("IHDR", "iTXt", "IDAT", "IEND"),
		"exif.png":         png("IHDR", "eXIf", "IDAT", "IEND"),
		"trailing.png":     png("IHDR", "IDAT", "IEND") + "made with x",
		"no-end.png":       png("IHDR", "IDAT"),
		"past-end.png":     png("IHDR") + "\xff\xff\xff\xf4IDATwhycrc!",
		"cut-off.png":      png("IHDR") + "\x00\x00",
		"not-an-image.png": "-- why\n",
	}
	for name, src := range refused {
		t.Run(name, func(t *testing.T) {
			path := write(t, name, src)
			if _, err := comments(path); err == nil || !strings.Contains(err.Error(), path) {
				t.Errorf("got %v, want an error that names %s", err, path)
			}
		})
	}
}

func TestTheCheckNeedsFilesToCheck(t *testing.T) {
	if code := run([]string{"-check"}); code != 2 {
		t.Fatalf("-check with no files exited %d, want 2", code)
	}
}
