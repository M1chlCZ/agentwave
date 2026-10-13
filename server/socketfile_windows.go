//go:build windows

package server

import "os"

func ownedSocket(os.FileInfo) bool {
	return true
}
