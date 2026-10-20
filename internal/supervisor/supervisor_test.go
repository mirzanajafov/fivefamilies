package supervisor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	switch os.Getenv("FF_SUPERVISOR_CHILD") {
	case "sleep":
		time.Sleep(time.Hour)
		return
	case "exit":
		time.Sleep(100 * time.Millisecond)
		os.Exit(3)
	}
	os.Exit(m.Run())
}

func child(t *testing.T, mode string) *Supervisor {
	t.Helper()
	t.Setenv("FF_SUPERVISOR_CHILD", mode)
	s := New([]string{os.Args[0], "-test.run=^$"})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Stop(os.Kill); err != nil {
			t.Error(err)
		}
	})
	return s
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("never: %s", what)
}

func TestAKilledNodeComesBackAfterTheFault(t *testing.T) {
	s := child(t, "sleep")
	if err := s.Kill(300 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if st := s.State(); !st.Killed || st.Until.IsZero() {
		t.Fatalf("state right after the kill: %+v", st)
	}
	eventually(t, "the node is not running while killed", func() bool { return !s.State().Running })
	eventually(t, "the node restarts", func() bool { st := s.State(); return st.Running && st.Starts == 2 && !st.Killed })
	select {
	case err := <-s.Failed():
		t.Fatalf("a kill we made was reported as the node exiting: %v", err)
	default:
	}
}

func TestARestartThatFailsIsReportedAsAFailure(t *testing.T) {
	s := child(t, "sleep")
	s.mu.Lock()
	s.args = []string{"ff-no-such-node-binary"}
	s.mu.Unlock()
	if err := s.Kill(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-s.Failed():
		if !strings.Contains(err.Error(), "starting the node") {
			t.Fatalf("reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the failed restart was not reported")
	}
	eventually(t, "the node stays down", func() bool { st := s.State(); return !st.Running && st.Killed })
}

func TestALateTimerDoesNotEndTheFaultThatReplacedIt(t *testing.T) {
	s := child(t, "sleep")
	held := false
	for range 20 {
		if err := s.Kill(50 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		if held = s.timer != nil; held {
			break
		}
		s.mu.Unlock()
		eventually(t, "the node restarts", func() bool { return s.State().Running })
	}
	if !held {
		t.Fatal("never took the lock before the kill's timer")
	}
	time.Sleep(500 * time.Millisecond)
	err := s.endFaultLocked()
	s.endAfterLocked(time.Minute)
	starts := s.state.Starts
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := s.State(); st.Until.IsZero() || st.Starts != starts {
		t.Fatalf("the kill's late timer ended the fault that replaced it: %+v", st)
	}
}

func TestHealEndsAFaultAtOnce(t *testing.T) {
	s := child(t, "sleep")
	if err := s.Kill(time.Minute); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the node stops", func() bool { return !s.State().Running })
	if err := s.Heal(); err != nil {
		t.Fatal(err)
	}
	if st := s.State(); !st.Running || st.Killed || !st.Until.IsZero() {
		t.Fatalf("after heal: %+v", st)
	}
}

func procState(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	return fields[0]
}

func TestAPausedNodeIsReallyStopped(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SIGSTOP is a Linux signal")
	}
	s := child(t, "sleep")
	pid := s.cmd.Process.Pid
	if err := s.Pause(300 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the kernel shows the process stopped", func() bool { return procState(pid) == "T" })
	eventually(t, "the process runs again", func() bool { return procState(pid) == "S" && !s.State().Paused })
	if s.State().Starts != 1 {
		t.Fatal("a pause restarted the node")
	}
}

func TestANodeThatExitsOnItsOwnIsReported(t *testing.T) {
	s := child(t, "exit")
	select {
	case err := <-s.Failed():
		if err == nil {
			t.Fatal("no error for an exit status of 3")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exit was not reported")
	}
}

func TestTheEndpointWantsTheTokenAndABoundedFault(t *testing.T) {
	s := child(t, "sleep")
	h := Handler(s, "secret")
	do := func(method, path, auth string) int {
		req := httptest.NewRequest(method, path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	cases := []struct {
		method, path, auth string
		want               int
	}{
		{"POST", "/kill?ms=100", "", http.StatusUnauthorized},
		{"POST", "/kill?ms=100", "Bearer wrong", http.StatusUnauthorized},
		{"POST", "/kill?ms=0", "Bearer secret", http.StatusBadRequest},
		{"POST", "/kill?ms=600000", "Bearer secret", http.StatusBadRequest},
		{"GET", "/state", "Bearer secret", http.StatusOK},
		{"POST", "/kill?ms=100", "Bearer secret", http.StatusOK},
		{"POST", "/kill?ms=100", "Bearer secret", http.StatusConflict},
		{"POST", "/heal", "Bearer secret", http.StatusOK},
	}
	for _, c := range cases {
		if got := do(c.method, c.path, c.auth); got != c.want {
			t.Fatalf("%s %s with %q: %d, want %d", c.method, c.path, c.auth, got, c.want)
		}
	}
	if code := do("GET", "/state", ""); code != http.StatusUnauthorized {
		t.Fatalf("state without a token: %d", code)
	}
	req := httptest.NewRequest("GET", "/state", nil)
	req.Header.Set("Authorization", "Bearer ")
	w := httptest.NewRecorder()
	Handler(s, "").ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("with no token configured, an empty bearer got %d", w.Code)
	}
}
