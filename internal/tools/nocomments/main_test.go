package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCommentsAreFoundInEveryKindOfFileTheRepoHolds(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]struct {
		src  string
		want []int
	}{
		"a.go":           {"package a\n\n// why\nfunc f() {}\n\nvar x = 1 /* why */\n", []int{3, 6}},
		"b.go":           {"package b\n\nvar url = \"http://example.com\"\n", nil},
		"c.ts":           {"const a = 'http://example.com'\n// why\nconst b = 1\n", []int{2}},
		"d.tsx":          {"<div>\n  {/* why */}\n</div>\n", []int{2}},
		"e.yml":          {"a: 1\n# why\nb: '#not-a-comment'\n", []int{2}},
		"f.sh":           {"#!/bin/sh\necho '#'\n  # why\n", []int{3}},
		"Dockerfile":     {"FROM scratch\n# why\n", []int{2}},
		"Dockerfile.net": {"FROM scratch\n", nil},
		"g.svg":          {"<svg>\n<!-- why -->\n</svg>\n", []int{2}},
		"h.json":         {"{\"a\": \"// not a comment\"}\n", nil},
	}
	for name, c := range cases {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(c.src), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := comments(path); err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: comments on lines %v, want %v: %v", name, got, c.want, err)
		}
	}
}
