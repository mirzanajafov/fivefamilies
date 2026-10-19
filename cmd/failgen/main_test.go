package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSamplesAreWrittenOneALineAndAFailedWriteIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	if err := writeSamples(path, []sample{{Node: 1, Ok: true}, {Node: 2}}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil || bytes.Count(b, []byte("\n")) != 2 {
		t.Fatalf("wrote %q: %v", b, err)
	}
	if err := writeSamples(t.TempDir(), []sample{{Node: 1}}); err == nil {
		t.Fatal("writing to a directory was not reported")
	}
}
