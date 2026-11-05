//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestProvider runs the spec named by E2E_PROVIDER ("all" = every spec in
// providers/) against the real core: `make e2e PROVIDER=strava`.
func TestProvider(t *testing.T) {
	name := os.Getenv("E2E_PROVIDER")
	if name == "" {
		t.Skip("E2E_PROVIDER unset: make e2e PROVIDER=<name> or make e2e-all")
	}
	sdkRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	specDir := filepath.Join(sdkRoot, "e2e", "providers")
	names := []string{name}
	if name == "all" {
		if names, err = ListSpecs(specDir); err != nil || len(names) == 0 {
			t.Fatalf("list specs: %v (%d)", err, len(names))
		}
	}
	specs := make([]*Spec, 0, len(names))
	for _, n := range names {
		s, err := LoadSpec(specDir, n)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runTimeout())
	defer cancel()
	stack := NewStack(sdkRoot, t.Logf)
	if err := stack.Up(ctx); err != nil {
		t.Fatalf("stack: %v", err)
	}
	defer stack.Down(context.Background())

	for _, spec := range specs {
		t.Run(spec.Provider, func(t *testing.T) {
			r := newRunner(ctx, t, spec, stack)
			r.runAll()
			path, err := r.writeReport()
			if err != nil {
				t.Errorf("write report: %v", err)
			}
			t.Logf("report: %s", path)
		})
	}
}

func runTimeout() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("E2E_TIMEOUT")); err == nil {
		return d
	}
	return 25 * time.Minute
}
