package supervisor

import (
	"os"
	"syscall"
)

func stop(p *os.Process) error {
	return p.Signal(syscall.SIGSTOP)
}

func cont(p *os.Process) error {
	return p.Signal(syscall.SIGCONT)
}
