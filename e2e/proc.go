//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// logTail keeps the last n lines of a process's output.
type logTail struct {
	mu    sync.Mutex
	lines []string
	n     int
}

func newLogTail(n int) *logTail { return &logTail{n: n} }

func (t *logTail) add(line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, line)
	if len(t.lines) > t.n {
		t.lines = t.lines[len(t.lines)-t.n:]
	}
}

// Lines returns a copy of the retained lines.
func (t *logTail) Lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.lines...)
}

// Contains reports whether any retained line contains s.
func (t *logTail) Contains(s string) bool {
	for _, l := range t.Lines() {
		if strings.Contains(l, s) {
			return true
		}
	}
	return false
}

// proc is a child process whose output goes to a log file and a tail.
type proc struct {
	name string
	cmd  *exec.Cmd
	tail *logTail
	log  *os.File
	done chan error
	err  error
}

// startProc runs name with args in dir and env, logging to <outDir>/<name>.log.
func startProc(ctx context.Context, outDir, name, dir string, env map[string]string, args ...string) (*proc, error) {
	logPath := filepath.Join(outDir, name+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // G304: the log lives in the harness's own out dir
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // G204: the binaries are built by the harness from the spec
	cmd.Dir = dir
	cmd.Env = mergedEnv(env)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second
	cmd.SysProcAttr = childAttr()
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	p := &proc{name: name, cmd: cmd, tail: newLogTail(200), log: logFile, done: make(chan error, 1)}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	go p.pump(pr)
	go func() {
		err := cmd.Wait()
		_ = pw.Close()
		p.done <- err
	}()
	return p, nil
}

// pump copies output lines to the log file and the tail.
func (p *proc) pump(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		p.tail.add(line)
		_, _ = p.log.WriteString(line + "\n")
	}
	_ = p.log.Close()
}

// Exited reports whether the process has ended (and the error if so).
func (p *proc) Exited() (bool, error) {
	select {
	case err := <-p.done:
		p.err = err
		p.done <- err
		return true, err
	default:
		return false, nil
	}
}

// Stop terminates the process and waits for it.
func (p *proc) Stop() {
	if p == nil || p.cmd.Process == nil {
		return
	}
	if exited, _ := p.Exited(); exited {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-p.done:
		p.err = err
		p.done <- err
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		p.err = <-p.done
		p.done <- p.err
	}
}

// exitError wraps the process's exit with errExited and its last lines.
func (p *proc) exitError(err error, n int) error {
	return fmt.Errorf("%s %w\n%s", p.name, errors.Join(errExited, err), strings.Join(p.Tail(n), "\n"))
}

// Tail returns the last lines of output.
func (p *proc) Tail(n int) []string {
	if p == nil {
		return nil
	}
	lines := p.tail.Lines()
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// mergedEnv is the current environment with env applied on top.
func mergedEnv(env map[string]string) []string {
	out := os.Environ()
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

// run executes a command to completion and returns its stdout; stderr
// (podman compose chatter, compiler output) only travels with an error.
func run(ctx context.Context, dir string, env map[string]string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // G204: fixed tools (go, git, podman) with harness-built arguments
	cmd.Dir = dir
	cmd.Env = mergedEnv(env)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s: %w\n%s%s", strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String(), nil
}

// errExited is returned by waits when the watched process died.
var errExited = errors.New("process exited")
