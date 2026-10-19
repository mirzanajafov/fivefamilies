package main

import (
	"slices"
	"testing"
)

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
