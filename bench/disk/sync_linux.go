package main

import (
	"os"
	"syscall"
)

func datasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

func preallocate(f *os.File, size int64) error {
	return syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
