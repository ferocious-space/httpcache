package httpcache

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testCache is a correct, minimal Cache used to isolate Transport behaviour.
type testCache struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newTestCache() *testCache { return &testCache{m: map[string][]byte{}} }

func (c *testCache) Get(k string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[k]
	return v, ok
}

func (c *testCache) Set(k string, v []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = v
}

func (c *testCache) Delete(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, k)
}

func (c *testCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

func TestFreshResponseServedFromCache(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		fmt.Fprint(w, "cacheable")
	}))
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()
	for i := 0; i < 3; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != "cacheable" {
			t.Fatalf("request %d body = %q, want %q", i+1, b, "cacheable")
		}
		if i > 0 && resp.Header.Get(XFromCache) == "" {
			t.Errorf("request %d was not marked with %s", i+1, XFromCache)
		}
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("upstream hits = %d, want 1", got)
	}
}

// Defect 2: every concurrent caller must receive its own readable body.
func TestConcurrentGetsEachGetTheBody(t *testing.T) {
	const body = "PAYLOAD-1234567890"
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(200 * time.Millisecond) // widen the dedup window
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()

	const n = 4
	var wg sync.WaitGroup
	bodies := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Get(srv.URL)
			if err != nil {
				errs[i] = err
				return
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			errs[i], bodies[i] = err, string(b)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("caller %d: %v", i, errs[i])
		}
		if bodies[i] != body {
			t.Errorf("caller %d body = %q, want %q", i, bodies[i], body)
		}
	}
	// Deviation: a request with nothing cached behind it is NOT deduplicated,
	// so each caller reaches the origin. Sharing here would mean buffering an
	// unbounded, unknown-size body in memory and withholding every byte from
	// the caller until the origin finished — which breaks large downloads and
	// streaming endpoints outright. Dedup applies to revalidation instead,
	// where the body is a 304 or is about to be cached anyway; see
	// TestConcurrentRevalidationsAreDeduplicated.
	if got := atomic.LoadInt64(&hits); got != n {
		t.Errorf("upstream hits = %d, want %d (uncached requests must not be deduplicated)", got, n)
	}
}

// Dedup must still collapse a stampede of concurrent revalidations of one
// stale cache entry — the case a cache actually faces, and the reason the
// singleflight is there at all.
func TestConcurrentRevalidationsAreDeduplicated(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Etag", `"v1"`)
		w.Header().Set("Cache-Control", "max-age=1")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		if r.Header.Get("if-none-match") == `"v1"` {
			time.Sleep(200 * time.Millisecond) // hold the flight open
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fmt.Fprint(w, "revalidated-body")
	}))
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()

	// Prime the cache, then let it go stale.
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("priming took %d requests, want 1", got)
	}
	time.Sleep(1100 * time.Millisecond)

	const n = 4
	var wg sync.WaitGroup
	bodies := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
				return
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Errorf("caller %d reading body: %v", i, err)
			}
			bodies[i] = string(b)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if bodies[i] != "revalidated-body" {
			t.Errorf("caller %d body = %q, want %q", i, bodies[i], "revalidated-body")
		}
	}
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Errorf("origin saw %d requests, want 2 (1 priming + 1 shared revalidation for %d concurrent callers)", got, n)
	}
}

// Defect 1: non-cacheable methods must never be deduplicated. Two concurrent
// POSTs with different bodies must both reach the origin.
func TestConcurrentPostsAreNotDeduplicated(t *testing.T) {
	var seen sync.Map
	var count int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen.Store(string(b), true)
		atomic.AddInt64(&count, 1)
		time.Sleep(150 * time.Millisecond)
		fmt.Fprintf(w, "reply-to:%s", b)
	}))
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()
	payloads := []string{"ORDER-A", "ORDER-B"}
	replies := make([]string, len(payloads))

	var wg sync.WaitGroup
	for i, p := range payloads {
		wg.Add(1)
		go func(i int, p string) {
			defer wg.Done()
			resp, err := client.Post(srv.URL, "text/plain", strings.NewReader(p))
			if err != nil {
				t.Errorf("POST %s: %v", p, err)
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			replies[i] = string(b)
		}(i, p)
	}
	wg.Wait()

	if got := atomic.LoadInt64(&count); got != 2 {
		t.Errorf("origin saw %d POSTs, want 2 (a request was silently dropped)", got)
	}
	for _, p := range payloads {
		if _, ok := seen.Load(p); !ok {
			t.Errorf("POST body %q never reached the origin", p)
		}
	}
	for i, p := range payloads {
		if want := "reply-to:" + p; replies[i] != want {
			t.Errorf("caller %d got %q, want %q", i, replies[i], want)
		}
	}
}

// Defect 12: requests differing in Authorization must never share a response.
func TestDedupDoesNotCrossHeaderBoundaries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, "private-data-for:%s", r.Header.Get("Authorization"))
	}))
	defer srv.Close()

	tr := NewTransport(newTestCache())
	users := []string{"alice-token", "bob-token"}
	got := make([]string, len(users))

	var wg sync.WaitGroup
	for i, u := range users {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", srv.URL, nil)
			req.Header.Set("Authorization", u)
			resp, err := tr.RoundTrip(req)
			if err != nil {
				t.Errorf("user %s: %v", u, err)
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			got[i] = string(b)
		}(i, u)
	}
	wg.Wait()

	for i, u := range users {
		if want := "private-data-for:" + u; got[i] != want {
			t.Errorf("user %s received %q, want %q", u, got[i], want)
		}
	}
}

// Defect 6: the default transport must be shared so connections are pooled.
func TestDefaultTransportReusesConnections(t *testing.T) {
	var accepted int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprint(w, "x")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt64(&accepted, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()
	for i := 0; i < 5; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if got := atomic.LoadInt64(&accepted); got != 1 {
		t.Errorf("5 sequential requests opened %d connections, want 1", got)
	}
}

// Defect 7: the locally synthesized 504 must never be stored.
func TestOnlyIfCachedMissIsNotStored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "real body")
	}))
	defer srv.Close()

	c := newTestCache()
	tr := NewTransport(c)

	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Cache-Control", "only-if-cached")
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}
	if c.len() != 0 {
		v, _ := c.Get(srv.URL)
		t.Errorf("cache holds %d entries after an only-if-cached miss: %q", c.len(), v)
	}
}

// Defect 10: a no-cache response must be revalidated on every use.
func TestResponseNoCacheForcesRevalidation(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Expires", time.Now().UTC().Add(time.Hour).Format(time.RFC1123))
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		fmt.Fprintf(w, "version-%d", n)
	}))
	defer srv.Close()

	client := NewTransport(newTestCache()).Client()
	for i := 0; i < 3; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if got := atomic.LoadInt64(&hits); got != 3 {
		t.Errorf("upstream hits = %d, want 3; a no-cache response was reused without revalidating", got)
	}
}

// Defect 11: a request's max-age must not extend freshness past the origin's.
func TestRequestMaxAgeCannotExtendFreshness(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=1")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		fmt.Fprint(w, "short-lived")
	}))
	defer srv.Close()

	tr := NewTransport(newTestCache())

	req, _ := http.NewRequest("GET", srv.URL, nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	time.Sleep(2100 * time.Millisecond) // outlive max-age=1

	req2, _ := http.NewRequest("GET", srv.URL, nil)
	req2.Header.Set("Cache-Control", "max-age=3600")
	resp2, err := tr.RoundTrip(req2)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Errorf("upstream hits = %d, want 2; request max-age extended the origin's lifetime", got)
	}
}

// A request's max-age must still be able to tighten freshness.
func TestRequestMaxAgeCanTightenFreshness(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		fmt.Fprint(w, "long-lived")
	}))
	defer srv.Close()

	tr := NewTransport(newTestCache())

	req, _ := http.NewRequest("GET", srv.URL, nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	time.Sleep(1100 * time.Millisecond)

	req2, _ := http.NewRequest("GET", srv.URL, nil)
	req2.Header.Set("Cache-Control", "max-age=1") // stricter than the origin
	resp2, err := tr.RoundTrip(req2)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Errorf("upstream hits = %d, want 2; a stricter request max-age was ignored", got)
	}
}

// flightKey must be unambiguously framed: a header value that happens to
// contain the framing bytes must not be able to imitate a different header
// set. Two distinct header sets sharing a digest would share one singleflight
// slot, and one caller would receive another caller's response.
func TestFlightKeyIsUnambiguous(t *testing.T) {
	const url = "https://api.example.com/me"

	mk := func(h http.Header) string {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = h
		return flightKey(url, req)
	}

	cases := []struct {
		name string
		a, b http.Header
	}{
		{
			"multi-value vs single value containing the value separator",
			http.Header{"A": {"x", "y"}},
			http.Header{"A": {"x\x01y"}},
		},
		{
			"two headers vs one crafted value imitating both",
			http.Header{"A": {"v"}, "B": {"w"}},
			http.Header{"A": {"v\x00B\x01w"}},
		},
		{
			"value boundary shifted between adjacent values",
			http.Header{"A": {"xy", "z"}},
			http.Header{"A": {"x", "yz"}},
		},
		{
			"name/value boundary shifted",
			http.Header{"Ab": {"c"}},
			http.Header{"A": {"bc"}},
		},
	}

	for _, tc := range cases {
		if ka, kb := mk(tc.a), mk(tc.b); ka == kb {
			t.Errorf("%s: distinct header sets produced the same flight key\n  a = %v\n  b = %v", tc.name, tc.a, tc.b)
		}
	}

	// Identical header sets must still collapse into one flight, or
	// deduplication stops working entirely.
	if mk(http.Header{"A": {"x"}, "B": {"y"}}) != mk(http.Header{"B": {"y"}, "A": {"x"}}) {
		t.Error("identical header sets produced different flight keys; deduplication would never trigger")
	}
}

// A response larger than MaxCacheableBytes must still reach the caller whole,
// but must not be stored — and must not be buffered while it passes through.
func TestOversizedResponseIsDeliveredButNotCached(t *testing.T) {
	const limit = 1 << 10
	body := bytes.Repeat([]byte("x"), limit*4)

	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		w.Write(body)
	}))
	defer srv.Close()

	c := newTestCache()
	client := NewTransport(c, WithMaxCacheableBytes(limit)).Client()

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("request %d: body was truncated: got %d bytes, want %d", i+1, len(got), len(body))
		}
	}

	if c.len() != 0 {
		t.Errorf("oversized response was cached: %d entries", c.len())
	}
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Errorf("upstream hits = %d, want 2 (nothing should have been served from cache)", got)
	}
}

// A response at or below the ceiling caches normally.
func TestResponseWithinLimitIsCached(t *testing.T) {
	const limit = 1 << 10
	body := bytes.Repeat([]byte("y"), limit) // exactly at the ceiling

	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
		w.Write(body)
	}))
	defer srv.Close()

	client := NewTransport(newTestCache(), WithMaxCacheableBytes(limit)).Client()
	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !bytes.Equal(got, body) {
			t.Fatalf("request %d: got %d bytes, want %d", i+1, len(got), len(body))
		}
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("upstream hits = %d, want 1 (second request should be a cache hit)", got)
	}
}

func TestMaxCacheableBytesDefaultsAndOverrides(t *testing.T) {
	if got := NewTransport(newTestCache()).maxCacheableBytes(); got != DefaultMaxCacheableBytes {
		t.Errorf("NewTransport default = %d, want %d", got, DefaultMaxCacheableBytes)
	}
	// A zero-valued Transport used directly must get the default too, not
	// "cache nothing".
	if got := (&Transport{}).maxCacheableBytes(); got != DefaultMaxCacheableBytes {
		t.Errorf("zero-valued Transport = %d, want %d", got, DefaultMaxCacheableBytes)
	}
	if got := NewTransport(newTestCache(), WithMaxCacheableBytes(42)).maxCacheableBytes(); got != 42 {
		t.Errorf("WithMaxCacheableBytes(42) = %d, want 42", got)
	}
	if got := NewTransport(newTestCache(), WithMaxCacheableBytes(-1)).maxCacheableBytes(); got >= 0 {
		t.Errorf("WithMaxCacheableBytes(-1) = %d, want a negative value meaning no ceiling", got)
	}
}
