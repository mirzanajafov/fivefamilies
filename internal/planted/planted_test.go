package planted

import (
	"math/bits"
	"testing"
)

func TestEveryBugHasItsOwnBitAndParsesFromTheNameItPrints(t *testing.T) {
	var all Bug
	for _, b := range All() {
		all |= b
		if got, err := Parse(b.String()); err != nil || got != b {
			t.Errorf("%s parsed back as %v: %v", b, got, err)
		}
	}
	if n := bits.OnesCount32(uint32(all)); n != len(All()) {
		t.Fatalf("%d bugs share %d bits", len(All()), n)
	}
}

func TestSeveralBugsParseTogetherAndNoneIsZero(t *testing.T) {
	if got, err := Parse("figure8, watchkeep"); err != nil || got != Figure8|WatchKeep {
		t.Fatalf("parsed %v: %v", got, err)
	}
	for _, s := range []string{"", "none"} {
		if got, err := Parse(s); err != nil || got != 0 {
			t.Fatalf("%q parsed as %v: %v", s, got, err)
		}
	}
}

func TestAnUnknownBugIsRefused(t *testing.T) {
	if got, err := Parse("figure8,figure9"); err == nil {
		t.Fatalf("parsed %v", got)
	}
}
