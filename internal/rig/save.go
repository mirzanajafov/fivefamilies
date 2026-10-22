package rig

import (
	"encoding/json"
	"os"

	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

func SaveJSON(dir, name string, v any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return vfs.WriteAtomic(vfs.OS{Dir: dir}, name, append(b, '\n'))
}
