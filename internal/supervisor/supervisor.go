package supervisor

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const MaxFault = time.Minute

type State struct {
	Running bool      `json:"running"`
	Paused  bool      `json:"paused"`
	Killed  bool      `json:"killed"`
	Until   time.Time `json:"until,omitzero"`
	Starts  int       `json:"starts"`
}

type Supervisor struct {
	args   []string
	mu     sync.Mutex
	cmd    *exec.Cmd
	state  State
	timer  *time.Timer
	end    func() error
	failed chan error
	ours   bool
}

func New(args []string) *Supervisor {
	return &Supervisor{args: args, failed: make(chan error, 1)}
}

func (s *Supervisor) Failed() <-chan error {
	return s.failed
}

func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startLocked()
}

func (s *Supervisor) startLocked() error {
	cmd := exec.Command(s.args[0], s.args[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor: starting the node: %w", err)
	}
	s.cmd, s.ours = cmd, false
	s.state.Running, s.state.Paused, s.state.Killed = true, false, false
	s.state.Starts++
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.cmd != cmd {
			return
		}
		s.state.Running = false
		if !s.ours {
			if err == nil {
				err = errors.New(cmd.ProcessState.String())
			}
			s.failLocked(fmt.Errorf("supervisor: the node exited on its own: %w", err))
		}
	}()
	return nil
}

func (s *Supervisor) failLocked(err error) {
	select {
	case s.failed <- err:
	default:
	}
}

func (s *Supervisor) endAfterLocked(d time.Duration, end func() error) {
	s.dropFaultLocked()
	s.end, s.state.Until = end, time.Now().Add(d)
	var t *time.Timer
	t = time.AfterFunc(d, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.timer == t {
			_ = s.endFaultLocked()
		}
	})
	s.timer = t
}

func (s *Supervisor) endFaultLocked() error {
	end := s.end
	s.dropFaultLocked()
	if end == nil {
		return nil
	}
	return end()
}

func (s *Supervisor) dropFaultLocked() {
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer, s.end, s.state.Until = nil, nil, time.Time{}
}

func (s *Supervisor) Kill(d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.state.Running || s.state.Killed {
		return errors.New("supervisor: the node is not running")
	}
	if s.state.Paused {
		cont(s.cmd.Process)
	}
	if err := s.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("supervisor: killing the node: %w", err)
	}
	s.ours = true
	s.state.Killed, s.state.Paused = true, false
	s.endAfterLocked(d, func() error {
		err := s.startLocked()
		if err != nil {
			s.failLocked(err)
		}
		return err
	})
	return nil
}

func (s *Supervisor) Pause(d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.state.Running || s.state.Killed || s.state.Paused {
		return errors.New("supervisor: the node is not running or is already paused")
	}
	if err := stop(s.cmd.Process); err != nil {
		return fmt.Errorf("supervisor: stopping the node: %w", err)
	}
	s.state.Paused = true
	s.endAfterLocked(d, func() error {
		s.state.Paused = false
		return cont(s.cmd.Process)
	})
	return nil
}

func (s *Supervisor) Heal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endFaultLocked()
}

func (s *Supervisor) Stop(sig os.Signal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ours = true
	s.dropFaultLocked()
	if s.cmd != nil && s.state.Running {
		if s.state.Paused {
			cont(s.cmd.Process)
		}
		s.cmd.Process.Signal(sig)
	}
}

func (s *Supervisor) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func Handler(s *Supervisor, token string) http.Handler {
	mux := http.NewServeMux()
	authed := func(w http.ResponseWriter, r *http.Request) bool {
		got := r.Header.Get("Authorization")
		if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return false
		}
		return true
	}
	fault := func(f func(time.Duration) error) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !authed(w, r) {
				return
			}
			ms, err := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
			d := time.Duration(ms) * time.Millisecond
			if err != nil || d <= 0 || d > MaxFault {
				http.Error(w, fmt.Sprintf("ms must be between 1 and %d", MaxFault.Milliseconds()), http.StatusBadRequest)
				return
			}
			if err := f(d); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			json.NewEncoder(w).Encode(s.State())
		}
	}
	mux.HandleFunc("POST /kill", fault(s.Kill))
	mux.HandleFunc("POST /pause", fault(s.Pause))
	mux.HandleFunc("POST /heal", func(w http.ResponseWriter, r *http.Request) {
		if !authed(w, r) {
			return
		}
		if err := s.Heal(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(s.State())
	})
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		if !authed(w, r) {
			return
		}
		json.NewEncoder(w).Encode(s.State())
	})
	return mux
}
