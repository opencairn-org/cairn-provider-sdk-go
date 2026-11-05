package workersdk

import (
	"strconv"
	"strings"
)

// parseParserVersion reads "N.M"; a legacy bare "N" reads as N.0.
func parseParserVersion(s string) (dataset, parser int, ok bool) {
	major, minor, hasMinor := strings.Cut(strings.TrimSpace(s), ".")
	d, err := strconv.Atoi(major)
	if err != nil || d < 0 {
		return 0, 0, false
	}
	if !hasMinor {
		return d, 0, true
	}
	p, err := strconv.Atoi(minor)
	if err != nil || p < 0 {
		return 0, 0, false
	}
	return d, p, true
}

// ParserAllows reports whether parser version own may re-parse a source last
// parsed by min (cairn-core ADR 0005): same dataset, own not older. An empty
// min allows; a non-N.M min only matches itself.
func ParserAllows(own, min string) bool {
	if min == "" {
		return true
	}
	md, mp, ok := parseParserVersion(min)
	if !ok {
		return own == min
	}
	od, op, ok := parseParserVersion(own)
	return ok && od == md && op >= mp
}
