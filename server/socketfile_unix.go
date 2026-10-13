//go:build unix

package server

import (
	"os"
	"syscall"
)

func ownedSocket(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
}
