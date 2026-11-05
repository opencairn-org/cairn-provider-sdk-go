// Package webhook verifies provider webhook signatures: HMAC-SHA256 over
// the raw body, or over a timestamp and the body, encoded as hex or base64,
// compared in constant time. The header carrying the signature, the shared
// secret and any "sha256=" style prefix are the worker's business; this
// package only answers "does this signature match this message".
//
//	// Polar, Ride with GPS, Hammerhead: hex HMAC of the body
//	ok := webhook.Verify(secret, env.Body, env.Headers["polar-webhook-signature"], webhook.Hex)
//	// WHOOP: base64 HMAC of timestamp + body
//	ok := webhook.VerifyTimestamped(secret, env.Headers["x-whoop-signature-timestamp"], env.Body,
//	    env.Headers["x-whoop-signature"], webhook.Base64)
//	// Oura: hex HMAC of timestamp + body
//	ok := webhook.VerifyTimestamped(secret, ts, env.Body, sig, webhook.Hex)
//
// Core forwards webhooks with lowercased header names
// (workersdk.WebhookEnvelope); a failed verification is a
// *port.TerminalError in the handler (bad_signature), never a retry.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Encoding is how a signature is rendered.
type Encoding int

const (
	// Hex is lowercase or uppercase hexadecimal.
	Hex Encoding = iota
	// Base64 is standard base64 with padding.
	Base64
	// Base64URL is URL-safe base64, padded or not.
	Base64URL
)

// Sign computes the HMAC-SHA256 of the concatenation of parts, rendered in
// enc. Use it to verify (Verify does) and in tests to sign fixtures.
func Sign(secret []byte, enc Encoding, parts ...[]byte) string {
	mac := hmac.New(sha256.New, secret)
	for _, p := range parts {
		mac.Write(p)
	}
	return encode(mac.Sum(nil), enc)
}

// Verify reports whether signature is the HMAC-SHA256 of body under
// secret. A scheme prefix ("sha256=" or "sha256:", GitHub style) is
// stripped; the comparison is constant-time on the decoded bytes.
func Verify(secret, body []byte, signature string, enc Encoding) bool {
	return VerifyParts(secret, signature, enc, body)
}

// VerifyTimestamped reports whether signature is the HMAC-SHA256 of
// timestamp followed by body (the string concatenation most providers use:
// WHOOP, Oura). For "v0:<ts>:<body>" styles pass the parts to VerifyParts.
func VerifyTimestamped(secret []byte, timestamp string, body []byte, signature string, enc Encoding) bool {
	return VerifyParts(secret, signature, enc, []byte(timestamp), body)
}

// VerifyParts reports whether signature is the HMAC-SHA256 of the
// concatenated parts under secret.
func VerifyParts(secret []byte, signature string, enc Encoding, parts ...[]byte) bool {
	want, ok := decode(stripScheme(signature), enc)
	if !ok || len(want) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	for _, p := range parts {
		mac.Write(p)
	}
	return hmac.Equal(mac.Sum(nil), want)
}

// FreshTimestamp reports whether a Unix timestamp (seconds or
// milliseconds, as providers send it) is within skew of now; it refuses
// an unparsable value. Replay protection for timestamped signatures.
func FreshTimestamp(timestamp string, now time.Time, skew time.Duration) bool {
	n, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil {
		return false
	}
	ts := time.Unix(n, 0)
	if n > 1e11 { // milliseconds
		ts = time.UnixMilli(n)
	}
	d := now.Sub(ts)
	if d < 0 {
		d = -d
	}
	return d <= skew
}

// stripScheme drops a leading "sha256=" / "sha256:" / "hmac-sha256=" label.
func stripScheme(sig string) string {
	sig = strings.TrimSpace(sig)
	if i := strings.IndexAny(sig, "=:"); i > 0 && strings.Contains(strings.ToLower(sig[:i]), "sha256") {
		return sig[i+1:]
	}
	return sig
}

func encode(mac []byte, enc Encoding) string {
	switch enc {
	case Base64:
		return base64.StdEncoding.EncodeToString(mac)
	case Base64URL:
		return base64.RawURLEncoding.EncodeToString(mac)
	default:
		return hex.EncodeToString(mac)
	}
}

func decode(sig string, enc Encoding) ([]byte, bool) {
	var (
		b   []byte
		err error
	)
	switch enc {
	case Base64:
		b, err = base64.StdEncoding.DecodeString(sig)
	case Base64URL:
		b, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(sig, "="))
	default:
		b, err = hex.DecodeString(sig)
	}
	return b, err == nil
}
