package workersdk

import "testing"

func TestParserAllows(t *testing.T) {
	for _, c := range []struct {
		own, min string
		want     bool
	}{
		{"1.1", "", true},
		{"1.1", "1", true},
		{"1.1", "1.1", true},
		{"1.1", "1.2", false},
		{"2.0", "1.4", false},
		{"1.4", "2.0", false},
		{"dev", "dev", true},
		{"1.1", "dev", false},
	} {
		if got := ParserAllows(c.own, c.min); got != c.want {
			t.Errorf("ParserAllows(%q, %q) = %v", c.own, c.min, got)
		}
	}
}
