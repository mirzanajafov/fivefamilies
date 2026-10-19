package main

import "os"

func datasync(f *os.File) error {
	return f.Sync()
}

func preallocate(f *os.File, size int64) error {
	return f.Truncate(size)
}
