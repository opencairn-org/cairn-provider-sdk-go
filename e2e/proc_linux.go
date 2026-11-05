//go:build e2e && linux

package e2e

import "syscall"

// childAttr ends a child with the harness, even when the test binary is killed.
func childAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM} }
