//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestProvider runs the spec named by E2E_PROVIDER ("all" = every spec in
// providers/) or the file named by E2E_SPEC against the real core:
// `make e2e PROVIDER=strava`, `make e2e SPEC=/abs/path/<name>.yaml`.
func TestProvider(t *testing.T) {
	sdkRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	specs, err := specsFromEnv(sdkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) == 0 {
		t.Skip("E2E_PROVIDER and E2E_SPEC unset: make e2e PROVIDER=<name> | SPEC=<file>, or make e2e-all")
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

// specsFromEnv loads E2E_SPEC (one file) or E2E_PROVIDER (a name under
// providers/, or "all").
func specsFromEnv(sdkRoot string) ([]*Spec, error) {
	if path := os.Getenv("E2E_SPEC"); path != "" {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("E2E_SPEC must be an absolute path, got %q", path)
		}
		s, err := LoadSpecFile(path)
		if err != nil {
			return nil, err
		}
		return []*Spec{s}, nil
	}
	name := os.Getenv("E2E_PROVIDER")
	if name == "" {
		return nil, nil
	}
	specDir := filepath.Join(sdkRoot, "e2e", "providers")
	names := []string{name}
	if name == "all" {
		var err error
		if names, err = ListSpecs(specDir); err != nil || len(names) == 0 {
			return nil, fmt.Errorf("list specs: %w (%d)", err, len(names))
		}
	}
	specs := make([]*Spec, 0, len(names))
	for _, n := range names {
		s, err := LoadSpec(specDir, n)
		if err != nil {
			return nil, err
		}
		specs = append(specs, s)
	}
	return specs, nil
}

func runTimeout() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("E2E_TIMEOUT")); err == nil {
		return d
	}
	return 25 * time.Minute
}
