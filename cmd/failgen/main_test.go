package main

import (
	"bytes"
	"errors"
	"testing"
)

type fullDisk struct{}

func (fullDisk) Write([]byte) (int, error) {
	return 0, errors.New("no space left on device")
}

func TestSamplesAreWrittenOneALineAndAFailedWriteIsReported(t *testing.T) {
	var b bytes.Buffer
	if err := encodeAll(&b, []sample{{Node: 1, Ok: true}, {Node: 2}}); err != nil || bytes.Count(b.Bytes(), []byte("\n")) != 2 {
		t.Fatalf("wrote %q: %v", b.String(), err)
	}
	if err := encodeAll(fullDisk{}, []sample{{Node: 1}}); err == nil {
		t.Fatal("a write that failed was not reported")
	}
}
