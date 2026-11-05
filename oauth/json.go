package oauth

import (
	"encoding/json"
	"io"
	"net/http"
)

// maxReply caps a token or profile reply (64 KiB).
const maxReply = 64 << 10

// decodeJSON reads a bounded JSON body.
func decodeJSON(resp *http.Response, v any) error {
	return json.NewDecoder(io.LimitReader(resp.Body, maxReply)).Decode(v)
}
