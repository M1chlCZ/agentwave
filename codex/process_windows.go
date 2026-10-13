//go:build windows

package codex

import (
	"os"
	"os/exec"
)

func prepareCommand(command *exec.Cmd) func() error {
	return func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		return command.Process.Kill()
	}
}
