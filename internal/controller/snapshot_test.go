package controller

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestTheDashboardsSnapshotFixtureIsWhatTheControllerWrites(t *testing.T) {
	want, err := os.ReadFile("testdata/snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(want))
	dec.DisallowUnknownFields()
	var s Snapshot
	if err := dec.Decode(&s); err != nil {
		t.Fatalf("the fixture has a field the controller doesn't write: %v", err)
	}
	got, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if got = append(got, '\n'); !bytes.Equal(got, want) {
		t.Fatalf("the controller writes the fixture's snapshot differently:\n%s", got)
	}
}
