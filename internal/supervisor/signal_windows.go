package supervisor

import (
	"errors"
	"os"
)

var errNoStop = errors.New("supervisor: pausing a process needs SIGSTOP, which Windows does not have")

func stop(*os.Process) error {
	return errNoStop
}

func cont(*os.Process) error {
	return errNoStop
}
