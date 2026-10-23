package main

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/png"
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

func chunk(kind, data string) string {
	return string(binary.BigEndian.AppendUint32(nil, uint32(len(data)))) + kind + data + "crc!"
}

func pngOf(kinds ...string) string {
	src := string(pngSignature)
	for _, kind := range kinds {
		switch kind {
		case "IHDR":
			src += chunk(kind, strings.Repeat("\x01", 13))
		case "IEND":
			src += chunk(kind, "")
		default:
			src += chunk(kind, "why")
		}
	}
	return src
}

func encodedPNG(t *testing.T) string {
	t.Helper()
	img := image.NewPaletted(image.Rect(0, 0, 4, 4), color.Palette{color.Black, color.White})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.String()
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

func TestPNGFilesMayHoldOnlyImageData(t *testing.T) {
	t.Chdir(t.TempDir())
	cases := map[string]struct {
		src     string
		refusal string
	}{
		"encoded.png":      {encodedPNG(t), ""},
		"chunks.png":       {pngOf("IHDR", "PLTE", "IDAT", "IDAT", "IEND"), ""},
		"text.png":         {pngOf("IHDR", "tEXt", "IDAT", "IEND"), `chunk 2 is "tEXt"`},
		"zipped.png":       {pngOf("IHDR", "IDAT", "zTXt", "IEND"), `chunk 3 is "zTXt"`},
		"unicode.png":      {pngOf("IHDR", "iTXt", "IDAT", "IEND"), `chunk 2 is "iTXt"`},
		"exif.png":         {pngOf("IHDR", "eXIf", "IDAT", "IEND"), `chunk 2 is "eXIf"`},
		"no-header.png":    {pngOf("IDAT", "IEND"), "IHDR comes first"},
		"two-headers.png":  {pngOf("IHDR", "IDAT", "IHDR", "IEND"), "IHDR comes first"},
		"long-header.png":  {string(pngSignature) + chunk("IHDR", strings.Repeat("\x01", 16)) + chunk("IEND", ""), "holds 16 bytes instead of 13"},
		"text-in-end.png":  {pngOf("IHDR", "IDAT") + chunk("IEND", "why"), "holds 3 bytes instead of 0"},
		"trailing.png":     {pngOf("IHDR", "IDAT", "IEND") + "made with x", "11 bytes after IEND"},
		"no-end.png":       {pngOf("IHDR", "IDAT"), "ends without IEND"},
		"past-end.png":     {pngOf("IHDR") + "\xff\xff\xff\xf4IDATwhycrc!", "runs past the end"},
		"cut-off.png":      {pngOf("IHDR") + "\x00\x00", "chunk 2 is cut off"},
		"not-an-image.png": {"-- why\n", "not a PNG"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			path := write(t, name, c.src)
			lines, err := comments(path)
			switch {
			case c.refusal == "" && (lines != nil || err != nil):
				t.Errorf("an image and nothing else gave %v, %v", lines, err)
			case c.refusal != "" && (err == nil || !strings.HasPrefix(err.Error(), path+": ") || !strings.Contains(err.Error(), c.refusal)):
				t.Errorf("got %v, want an error that names %s and says %q", err, path, c.refusal)
			}
		})
	}
}

func TestTheCheckNeedsFilesToCheck(t *testing.T) {
	if code := run([]string{"-check"}); code != 2 {
		t.Fatalf("-check with no files exited %d, want 2", code)
	}
}
