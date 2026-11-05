package workersdk

import (
	"context"
	"errors"
	"testing"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
)

func TestCoreReleaseCheck(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	newW := func(release string) *Worker {
		w, err := New(Config{Name: "w1", Provider: "strava", Release: release, Bus: bus})
		if err != nil {
			t.Fatal(err)
		}
		w.Handle(w.JobSubject("fetch_source"), func(context.Context, *Worker, Job) error { return nil })
		return w
	}
	if got := newW("v0.5.0").JobSubject("fetch_source"); got != "cairn.jobs.fetch_source.strava.w1" {
		t.Fatalf("subject = %q", got)
	}

	// No core info yet: not an error.
	if err := newW("v0.5.0").checkCoreRelease(ctx); err != nil {
		t.Fatal(err)
	}
	kv, _ := bus.KV(kvCoreInfo)
	if _, err := kv.Put(ctx, "info", []byte(`{"release":"v0.5.2"}`)); err != nil {
		t.Fatal(err)
	}
	for release, wantErr := range map[string]bool{"v0.5.9": false, "v0.4.3": true, "v1.5.0": true, "": false, "abc123": false} {
		if _, err := kv.Put(ctx, "info", []byte(`{"release":"v0.5.2"}`)); err != nil {
			t.Fatal(err)
		}
		err := newW(release).checkCoreRelease(ctx)
		if errors.Is(err, ErrIncompatibleCore) != wantErr {
			t.Errorf("%q: err = %v", release, err)
		}
	}
	// 1.x: an older worker minor works, a newer one does not.
	for _, c := range []struct {
		core, worker string
		want         bool
	}{
		{"v1.3.0", "v1.1.0", true}, {"v1.3.0", "v1.3.2", true}, {"v1.3.0", "v1.4.0", false}, {"v2.0.0", "v1.9.0", false},
		{"v0.5.0", "v0.6.0", false},
	} {
		if got := ReleaseCompatible(c.core, c.worker); got != c.want {
			t.Errorf("ReleaseCompatible(%s, %s) = %v", c.core, c.worker, got)
		}
	}
	// Run refuses to start against an incompatible core.
	if err := newW("v0.4.0").Run(ctx); !errors.Is(err, ErrIncompatibleCore) {
		t.Fatalf("Run: %v", err)
	}
}

func TestHandleWithMaxDeliver(t *testing.T) {
	w, err := New(Config{Name: "w1", Provider: "strava", Bus: inmem.New()})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleWithMaxDeliver(w.JobSubject("parse_blob"), func(context.Context, *Worker, Job) error { return nil }, 12)
	w.Handle(w.JobSubject("fetch_source"), func(context.Context, *Worker, Job) error { return nil })
	if w.maxDeliver[w.JobSubject("parse_blob")] != 12 || w.maxDeliver[w.JobSubject("fetch_source")] != 0 {
		t.Fatalf("maxDeliver = %v", w.maxDeliver)
	}
}
