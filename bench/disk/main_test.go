package main

import (
	"slices"
	"testing"
)

func TestOnlyKnownModesParse(t *testing.T) {
	if got, err := parseModes("append-fsync, prealloc-fdatasync"); err != nil || !slices.Equal(got, []string{"append-fsync", "prealloc-fdatasync"}) {
		t.Fatalf("parsed %v: %v", got, err)
	}
	for _, list := range []string{"", "apend-fsync", "prealloc-fdatasyn", "append-fsync,"} {
		if got, err := parseModes(list); err == nil {
			t.Errorf("%q parsed as %v", list, got)
		}
	}
}

func TestOnlyPositiveCountsParse(t *testing.T) {
	if got, err := counts("1, 5,16384"); err != nil || !slices.Equal(got, []int{1, 5, 16384}) {
		t.Fatalf("parsed %v: %v", got, err)
	}
	for _, list := range []string{"", "1,x", "0", "-5", "1,,2"} {
		if got, err := counts(list); err == nil {
			t.Errorf("%q parsed as %v", list, got)
		}
	}
}
