package sdktest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// BlobStore is the object store behind the presign responders: an
// httptest server that accepts PUT and serves GET on /<blob id>, keyed the
// way core keys blobs (users/<user>/<provider>/uploads/<n> for archives
// and attachments, transfer/<provider>/<n>.json for claim-checked results).
type BlobStore struct {
	srv *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	next    int
	failPut bool
}

func newBlobStore(t testing.TB) *BlobStore {
	t.Helper()
	b := &BlobStore{objects: map[string][]byte{}}
	b.srv = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(b.srv.Close)
	return b
}

// URL is the store's base URL.
func (b *BlobStore) URL() string { return b.srv.URL }

// Get returns a stored object.
func (b *BlobStore) Get(id string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[id]
	return data, ok
}

// Put stores an object under id (seed an archive for a parse_blob test).
func (b *BlobStore) Put(id string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[id] = data
}

// IDs lists the stored object ids.
func (b *BlobStore) IDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.objects))
	for id := range b.objects {
		out = append(out, id)
	}
	return out
}

// FailPuts makes every upload answer 503 (a blob store outage).
func (b *BlobStore) FailPuts(fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPut = fail
}

func (b *BlobStore) serve(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/")
	b.mu.Lock()
	defer b.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		if b.failPut {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		b.objects[id], _ = io.ReadAll(r.Body)
	case http.MethodGet:
		data, ok := b.objects[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// presignUpload mints a PUT URL; the key follows core's layout.
func (b *BlobStore) presignUpload(m *nats.Msg) {
	var req workersdk.PresignUploadRequest
	_ = json.Unmarshal(m.Data, &req)
	provider := req.Provider
	if provider == "" {
		provider = providerOf(m.Subject)
	}
	b.mu.Lock()
	b.next++
	n := b.next
	b.mu.Unlock()
	var id string
	switch {
	case req.Kind == "result":
		id = fmt.Sprintf("transfer/%s/%d.json", provider, n)
	case req.SourceID != "":
		id = fmt.Sprintf("users/%s/%s/sources/%s/%d", req.UserID, provider, req.SourceID, n)
	default:
		id = fmt.Sprintf("users/%s/%s/uploads/%d", req.UserID, provider, n)
	}
	reply(m, workersdk.PresignedURL{
		Method: http.MethodPut, URL: b.srv.URL + "/" + id, BlobID: id, ExpiresAt: time.Now().Add(15 * time.Minute),
		RequiredHeaders: map[string]string{"Content-Type": req.ContentType},
	})
}

// presignDownload mints a GET URL for an existing blob id.
func (b *BlobStore) presignDownload(m *nats.Msg) {
	var req workersdk.PresignDownloadRequest
	_ = json.Unmarshal(m.Data, &req)
	reply(m, workersdk.PresignedURL{Method: http.MethodGet, URL: b.srv.URL + "/" + req.BlobID, BlobID: req.BlobID, ExpiresAt: time.Now().Add(15 * time.Minute)})
}
