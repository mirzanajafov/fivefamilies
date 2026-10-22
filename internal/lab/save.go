package lab

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func SaveJSON(dir, name string, v any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644)
}
