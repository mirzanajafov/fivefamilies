package lab

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

func TestLabSurvivesFaults(t *testing.T) {
	if os.Getenv("FF_LAB") == "" {
		t.Skip("set FF_LAB=1 to run the five-node cluster in Docker with real faults")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	if err := rig.BuildLabImage(ctx, root); err != nil {
		t.Fatalf("building the node image: %v", err)
	}
	rep, err := Run(ctx, Options{
		Seed: uint64(time.Now().UnixNano()), Duration: 30 * time.Second, Settle: 3 * time.Second,
		Workers: 6, Keys: 3, Read: client.ReadIndex, Faults: AllFaults, FaultMean: 2 * time.Second,
		Election: "1s", Heartbeat: "100ms", Lease: "0s", OpTimeout: 2 * time.Minute, CheckTimeout: 5 * time.Minute,
		Project: "fflabtest", File: filepath.Join(root, "lab", "compose.yml"), Log: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seed %d: %d faults, %d operations, %d leader changes", rep.Options.Seed, len(rep.Faults), rep.Ops, len(rep.Leaders))
	for _, f := range rep.Faults {
		if f.Err != "" {
			t.Errorf("fault %s %s failed: %s", f.Kind, f.Detail, f.Err)
		}
	}
	if !rep.OK() {
		t.Fatalf("linearizable %s, states equal %v, problems %v", rep.Linearizable, rep.StatesEqual, rep.Problems)
	}
}

func TestTheLabComposeFileRunsTheImageTheLabBuilds(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join("..", "..", "lab", "compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(compose, []byte("image: "+rig.LabImage+"\n")) {
		t.Fatalf("lab/compose.yml doesn't run %s", rig.LabImage)
	}
}

func TestOnlyKnownFaultsParse(t *testing.T) {
	for s, want := range map[string][]FaultKind{"all": AllFaults, " none": nil, "kill, pause": {FaultKill, FaultPause}} {
		t.Run(fmt.Sprintf("%q", s), func(t *testing.T) {
			if got, err := ParseFaults(s); err != nil || !slices.Equal(got, want) {
				t.Errorf("parsed as %v: %v", got, err)
			}
		})
	}
	for _, s := range []string{"", "kil", "kill,partition"} {
		if got, err := ParseFaults(s); err == nil {
			t.Errorf("%q was accepted as %v", s, got)
		}
	}
}
