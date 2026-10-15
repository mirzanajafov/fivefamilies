package lab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLabSurvivesFaults(t *testing.T) {
	if os.Getenv("FF_LAB") == "" {
		t.Skip("set FF_LAB=1 to run the five-node cluster in Docker with real faults")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("docker", "build", "-q", "--build-arg", "LDFLAGS=-s -w -X main.plantable=yes", "-t", "fivefamilies-lab-node", root)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the node image: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rep, err := Run(ctx, Options{
		Seed: uint64(time.Now().UnixNano()), Duration: 30 * time.Second, Settle: 3 * time.Second,
		Workers: 6, Keys: 3, Read: "index", Faults: AllFaults, FaultMean: 2 * time.Second,
		Election: "1s", Heartbeat: "100ms", Lease: "0s", OpTimeout: 2 * time.Minute, CheckTimeout: 5 * time.Minute,
		Project: "fflabtest", File: filepath.Join(root, "lab", "compose.yml"),
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
