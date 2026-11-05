//go:build e2e && !linux

package e2e

import "syscall"

func childAttr() *syscall.SysProcAttr { return nil }
