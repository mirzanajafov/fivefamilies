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
		t.Fatalf("the fixture doesn't decode as the controller's snapshot: %v", err)
	}
	got, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if !bytes.Equal(got, want) {
		t.Fatalf("the controller writes the fixture's snapshot differently:\n%s", got)
	}
}
