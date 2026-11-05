// Package fakeprovider is the HTTP API of "Acme", the imaginary fitness
// platform examples/minimal imports from. It has the shape most real
// providers share — OAuth 2 with rotating refresh tokens, a profile call
// for the account id, a paged activity list, a detail call, an original FIT
// file per activity and a daily-metric endpoint — so the example worker can
// be read as a template. Tests run it with httptest.
package fakeprovider

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/muktihari/fit/encoder"
	"github.com/muktihari/fit/profile/filedef"
	"github.com/muktihari/fit/profile/mesgdef"
	"github.com/muktihari/fit/profile/typedef"
)

// Activity is one stored workout.
type Activity struct {
	ID    string    `json:"id"`
	Owner string    `json:"-"`
	Name  string    `json:"name"`
	Start time.Time `json:"start_time"`
	FIT   []byte    `json:"-"`
}

// Metric is one daily value.
type Metric struct {
	Owner string    `json:"-"`
	Key   string    `json:"key"`
	TS    time.Time `json:"ts"`
	Value float64   `json:"value"`
}

// Server is the in-memory provider.
type Server struct {
	mu         sync.Mutex
	activities []Activity
	metrics    []Metric
	access     map[string]string // access token → owner
	refresh    map[string]string // refresh token → owner
	codes      map[string]string // authorization code → owner
	Refreshes  int
	Calls      int
}

// New returns an empty provider.
func New() *Server {
	return &Server{access: map[string]string{}, refresh: map[string]string{}, codes: map[string]string{}}
}

// AddActivity stores a workout for owner with a generated FIT file.
func (s *Server) AddActivity(owner, id, name string, start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activities = append(s.activities, Activity{ID: id, Owner: owner, Name: name, Start: start, FIT: RideFIT(start)})
}

// AddMetric stores a daily value for owner.
func (s *Server) AddMetric(owner, key string, ts time.Time, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, Metric{Owner: owner, Key: key, TS: ts, Value: value})
}

// GrantCode makes an authorization code exchangeable for owner's tokens.
func (s *Server) GrantCode(code, owner string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes[code] = owner
}

// IssueToken mints an access/refresh pair for owner directly (seeding
// core's token store in tests).
func (s *Server) IssueToken(owner string) (access, refresh string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issue(owner)
}

// RevokeRefresh invalidates a refresh token (the user de-authorised).
func (s *Server) RevokeRefresh(refresh string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.refresh, refresh)
}

func (s *Server) issue(owner string) (access, refresh string) {
	access, refresh = "at-"+random(), "rt-"+random()
	s.access[access], s.refresh[refresh] = owner, owner
	return access, refresh
}

func random() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Handler serves the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", s.token)
	mux.HandleFunc("GET /v1/me", s.auth(s.me))
	mux.HandleFunc("GET /v1/activities", s.auth(s.list))
	mux.HandleFunc("GET /v1/activities/{id}", s.auth(s.detail))
	mux.HandleFunc("GET /v1/activities/{id}/file.fit", s.auth(s.file))
	mux.HandleFunc("GET /v1/metrics", s.auth(s.listMetrics))
	return mux
}

// token is the OAuth token endpoint: code or refresh_token grants, client
// credentials in the body, refresh tokens single-use and rotated.
func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Form.Get("client_id") == "" || r.Form.Get("client_secret") == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
		return
	}
	var owner string
	var ok bool
	switch r.Form.Get("grant_type") {
	case "authorization_code":
		owner, ok = s.codes[r.Form.Get("code")]
		delete(s.codes, r.Form.Get("code"))
	case "refresh_token":
		owner, ok = s.refresh[r.Form.Get("refresh_token")]
		delete(s.refresh, r.Form.Get("refresh_token"))
		s.Refreshes++
	}
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
		return
	}
	access, refresh := s.issue(owner)
	writeJSON(w, http.StatusOK, map[string]any{"access_token": access, "refresh_token": refresh, "token_type": "bearer", "expires_in": 3600, "scope": "activity:read"})
}

// auth checks the bearer token and passes the owner on.
func (s *Server) auth(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.Calls++
		owner, ok := s.access[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		s.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "99")
		next(w, r, owner)
	}
}

func (s *Server) me(w http.ResponseWriter, _ *http.Request, owner string) {
	writeJSON(w, http.StatusOK, map[string]string{"id": owner})
}

// list pages the owner's activities after a Unix time, newest last.
func (s *Server) list(w http.ResponseWriter, r *http.Request, owner string) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	page, perPage = max(page, 1), max(perPage, 1)
	s.mu.Lock()
	var own []Activity
	for _, a := range s.activities {
		if a.Owner == owner && a.Start.Unix() > after {
			own = append(own, a)
		}
	}
	s.mu.Unlock()
	sort.Slice(own, func(i, j int) bool { return own[i].Start.Before(own[j].Start) })
	from := min((page-1)*perPage, len(own))
	to := min(from+perPage, len(own))
	out := own[from:to]
	if out == nil {
		out = []Activity{}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) find(id, owner string) (Activity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.activities {
		if a.ID == id && a.Owner == owner {
			return a, true
		}
	}
	return Activity{}, false
}

func (s *Server) detail(w http.ResponseWriter, r *http.Request, owner string) {
	a, ok := s.find(r.PathValue("id"), owner)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) file(w http.ResponseWriter, r *http.Request, owner string) {
	a, ok := s.find(r.PathValue("id"), owner)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	w.Header().Set("Content-Type", "application/vnd.ant.fit")
	_, _ = w.Write(a.FIT)
}

func (s *Server) listMetrics(w http.ResponseWriter, r *http.Request, owner string) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	until, _ := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
	s.mu.Lock()
	out := []Metric{}
	for _, m := range s.metrics {
		if m.Owner == owner && m.TS.Unix() >= since && m.TS.Unix() < until {
			out = append(out, m)
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// RideFIT encodes a small road ride starting at start: 30 samples at 1 Hz
// with GPS, heart rate and power, one session and one lap.
func RideFIT(start time.Time) []byte {
	a := filedef.NewActivity()
	a.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileActivity).SetManufacturer(typedef.ManufacturerDevelopment).SetProduct(1).SetProductName("Acme Head Unit").SetTimeCreated(start)
	const n = 30
	a.Sessions = []*mesgdef.Session{mesgdef.NewSession(nil).SetStartTime(start).SetTimestamp(start.Add(n * time.Second)).
		SetSport(typedef.SportCycling).SetSubSport(typedef.SubSportRoad).SetTotalElapsedTime(n * 1000).SetTotalTimerTime(n * 1000).
		SetTotalDistance(n * 10 * 100).SetAvgHeartRate(140).SetAvgPower(200).SetTotalAscent(12)}
	a.Laps = []*mesgdef.Lap{mesgdef.NewLap(nil).SetStartTime(start).SetTimestamp(start.Add(n * time.Second)).SetTotalElapsedTime(n * 1000).SetTotalDistance(n * 10 * 100)}
	for i := 0; i < n; i++ {
		a.Records = append(a.Records, mesgdef.NewRecord(nil).SetTimestamp(start.Add(time.Duration(i)*time.Second)).
			SetPositionLatDegrees(48.1+float64(i)*0.0001).SetPositionLongDegrees(11.5+float64(i)*0.0001).
			SetDistance(uint32(i*10*100)).SetHeartRate(uint8(130+i%20)).SetPower(uint16(180+i))) //nolint:gosec // G115: small fixture values
	}
	fit := a.ToFIT(nil)
	var buf bytes.Buffer
	_ = encoder.New(&buf).Encode(&fit)
	return buf.Bytes()
}
