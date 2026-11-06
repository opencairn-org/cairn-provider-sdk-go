package webhook

import (
	"strings"
	"testing"
	"time"
)

func TestVerifyBody(t *testing.T) {
	secret := []byte("s3cret")
	body := []byte(`{"event":"workout.updated"}`)
	for _, enc := range []Encoding{Hex, Base64, Base64URL} {
		sig := Sign(secret, enc, body)
		if !Verify(secret, body, sig, enc) {
			t.Errorf("enc %d: valid signature rejected", enc)
		}
		if Verify([]byte("other"), body, sig, enc) || Verify(secret, []byte("tampered"), sig, enc) {
			t.Errorf("enc %d: forged signature accepted", enc)
		}
		if Verify(secret, body, sig, (enc+1)%3) {
			t.Errorf("enc %d: wrong encoding accepted", enc)
		}
	}
	hexSig := Sign(secret, Hex, body)
	for _, sig := range []string{"sha256=" + hexSig, "sha256:" + hexSig, " " + strings.ToUpper(hexSig) + " ", "HMAC-SHA256=" + hexSig} {
		if !Verify(secret, body, sig, Hex) {
			t.Errorf("%q: labelled signature rejected", sig)
		}
	}
	for _, sig := range []string{"", "zz", "sha256=", hexSig[:10], "md5=" + hexSig} {
		if Verify(secret, body, sig, Hex) {
			t.Errorf("%q: malformed signature accepted", sig)
		}
	}
	if !Verify(secret, body, Sign(secret, Base64URL, body)+"=", Base64URL) {
		t.Error("padded base64url rejected")
	}
}

func TestVerifyTimestamped(t *testing.T) {
	secret := []byte("whoop")
	body := []byte(`{"id":1}`)
	ts := "1700000000000"
	sig := Sign(secret, Base64, []byte(ts), body)
	if !VerifyTimestamped(secret, ts, body, sig, Base64) {
		t.Error("valid timestamped signature rejected")
	}
	if VerifyTimestamped(secret, "1700000000001", body, sig, Base64) {
		t.Error("replayed timestamp accepted")
	}
	// Slack-style v0:ts:body
	parts := [][]byte{[]byte("v0:"), []byte(ts), []byte(":"), body}
	if !VerifyParts(secret, Sign(secret, Hex, parts...), Hex, parts...) {
		t.Error("multi-part signature rejected")
	}
}

func TestFreshTimestamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sec := now.Unix()
	cases := map[string]bool{
		"1767268800":    true,  // exact seconds
		"1767268740":    true,  // 60s ago
		"1767268800000": true,  // milliseconds
		"1767268000":    false, // 13 minutes ago
		"1767268500":    true,  // exactly the skew
		"1767268860":    true,  // 60s ahead (clock drift)
		"1767269600":    false, // 13 minutes ahead
		"1767269600000": false, // 13 minutes ahead, milliseconds
		"abc":           false,
		"":              false,
	}
	if cases["1767268800"] && sec != 1767268800 {
		t.Fatalf("fixture drift: now = %d", sec)
	}
	for ts, want := range cases {
		if got := FreshTimestamp(ts, now, 5*time.Minute); got != want {
			t.Errorf("FreshTimestamp(%q) = %v", ts, got)
		}
	}
}
