package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func write(t *testing.T, dir, name, src string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCommentsAreFoundInEveryKindOfFileTheRepoHolds(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]struct {
		src  string
		want []int
	}{
		"a.go":            {"package a\n\n// why\nfunc f() {}\n\nvar x = 1 /* why */\n", []int{3, 6}},
		"b.go":            {"package b\n\nvar url = \"http://example.com\"\n", nil},
		"c.ts":            {"const a = 'http://example.com'\n// why\nconst b = 1\n", []int{2}},
		"d.tsx":           {"<div>\n  {/* why */}\n  { /* why */ }\n</div>\n", []int{2, 3}},
		"e.mjs":           {"\xef\xbb\xbf// why\nexport default {}\n", []int{1}},
		"f.proto":         {"syntax = \"proto3\";\n// why\n", []int{2}},
		"g.css":           {"a { color: red; } /* why */\n", []int{1}},
		"h.yml":           {"a: 1\n# why\nb: '#not-a-comment'\n", []int{2}},
		"i.yaml":          {"#! why\n", []int{1}},
		"j.sh":            {"#!/bin/sh\necho '#'\n  # why\n", []int{3}},
		"Dockerfile":      {"FROM scratch\n# why\n", []int{2}},
		"Dockerfile.net":  {"#! why\nFROM scratch\n", []int{1}},
		"web.dockerfile":  {"# why\n", []int{1}},
		".gitignore":      {"node_modules\n# why\n", []int{2}},
		".dockerignore":   {"# why\n", []int{1}},
		".gitattributes":  {"# why\n", []int{1}},
		".prettierignore": {"# why\n", []int{1}},
		"go.mod":          {"module x\n\n// why\n", []int{3}},
		"tsconfig.json":   {"{\n  // why\n}\n", []int{2}},
		"k.svg":           {"<svg><!-- why --><path/></svg>\n", []int{1}},
		"l.json":          {"{\"a\": \"// not a comment\"}\n", nil},
		"go.sum":          {"example.com v1.0.0 h1:x\n", nil},
		"cpu.txt":         {"# not a comment in profile output\n", nil},
	}
	for name, c := range cases {
		got, err := comments(write(t, dir, name, c.src))
		if err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: comments on lines %v, want %v: %v", name, got, c.want, err)
		}
	}
}

func TestAFileWithNoCommentRuleFailsTheCheck(t *testing.T) {
	dir := t.TempDir()
	clean := write(t, dir, "a.go", "package a\n")
	unknown := write(t, dir, "schema.sql", "-- why\n")
	if n := check([]string{clean}); n != 0 {
		t.Fatalf("a clean file counted %d problems", n)
	}
	if n := check([]string{clean, unknown}); n != 1 {
		t.Fatalf("a file with no rule counted %d problems, want 1", n)
	}
}
