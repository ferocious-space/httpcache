package integration

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ferocious-space/httpcache"
)

// A caller must receive bytes as the origin produces them, not after it
// finishes. /slow-stream emits a chunk, stalls, then finishes.
func TestStreamingIsNotBlockedByCaching(t *testing.T) {
	h := newHarness(t, false, false)

	start := time.Now()
	resp, err := h.client.Get(h.URL("/slow-stream"))
	if err != nil {
		t.Fatal(err)
	}
	headersAt := time.Since(start)

	buf := make([]byte, len("FIRST-CHUNK"))
	n, err := io.ReadFull(resp.Body, buf)
	firstChunkAt := time.Since(start)
	if err != nil {
		t.Fatalf("reading first chunk: %v", err)
	}

	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	t.Logf("headers at %v, first %d body bytes at %v, origin stalls %v mid-body",
		headersAt.Round(10*time.Millisecond), n, firstChunkAt.Round(10*time.Millisecond), stallFor)

	if firstChunkAt >= stallFor {
		t.Errorf("first chunk took %v: the response was buffered before the caller saw it", firstChunkAt)
	}
	if got, want := string(buf)+string(rest), "FIRST-CHUNKLAST-CHUNK"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}

	// Having been read to EOF, it must now be cached.
	resp2, body2, err := getBody(h, "/slow-stream")
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache(resp2) {
		t.Errorf("fully-read streamed response was not cached")
	}
	if string(body2) != "FIRST-CHUNKLAST-CHUNK" {
		t.Errorf("cached body = %q, want the full body", body2)
	}
	if got := h.origin.count("/slow-stream"); got != 1 {
		t.Errorf("origin hits = %d, want 1", got)
	}
}

func getBody(h *harness, path string) (*http.Response, []byte, error) {
	resp, err := h.client.Get(h.URL(path))
	if err != nil {
		return nil, nil, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body, err
}

// A body over the ceiling is delivered whole but never stored.
func TestOversizedResponseNotCached(t *testing.T) {
	const limit = 64 << 10
	h := newHarness(t, false, false, httpcache.WithMaxCacheableBytes(limit))

	path := fmt.Sprintf("/large?size=%d", limit*4)
	for i := 0; i < 2; i++ {
		resp, body, err := getBody(h, path)
		if err != nil {
			t.Fatal(err)
		}
		if len(body) != limit*4 {
			t.Fatalf("request %d: body = %d bytes, want %d", i+1, len(body), limit*4)
		}
		if fromCache(resp) {
			t.Errorf("request %d was served from cache despite exceeding the ceiling", i+1)
		}
	}
	if got := h.origin.count("/large"); got != 2 {
		t.Errorf("origin hits = %d, want 2", got)
	}
	if h.cache.Size() != 0 {
		t.Errorf("cache holds %d bytes, want 0", h.cache.Size())
	}
}

// A body under the ceiling caches normally.
func TestUndersizedResponseIsCached(t *testing.T) {
	const limit = 64 << 10
	h := newHarness(t, false, false, httpcache.WithMaxCacheableBytes(limit))

	path := fmt.Sprintf("/large?size=%d", limit/4)
	for i := 0; i < 2; i++ {
		if _, _, err := getBody(h, path); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.origin.count("/large"); got != 1 {
		t.Errorf("origin hits = %d, want 1 (second request should be a cache hit)", got)
	}
	if h.cache.Size() == 0 {
		t.Error("cache is empty; the response should have been stored")
	}
}

// A stale entry revalidated by many callers at once must produce one origin
// request, and every caller must get the right body.
func TestConcurrentRevalidationHitsOriginOnce(t *testing.T) {
	h := newHarness(t, false, false)

	if _, body, err := getBody(h, "/etag"); err != nil {
		t.Fatal(err)
	} else if string(body) != "etag-body" {
		t.Fatalf("priming body = %q, want %q", body, "etag-body")
	}
	if got := h.origin.count("/etag"); got != 1 {
		t.Fatalf("priming hits = %d, want 1", got)
	}

	time.Sleep(1100 * time.Millisecond) // outlive max-age=1

	const n = 6
	var wg sync.WaitGroup
	bodies := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, body, err := getBody(h, "/etag")
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
				return
			}
			bodies[i] = string(body)
		}(i)
	}
	wg.Wait()

	for i, b := range bodies {
		if b != "etag-body" {
			t.Errorf("caller %d body = %q, want %q", i, b, "etag-body")
		}
	}
	if got := h.origin.count("/etag"); got != 2 {
		t.Errorf("origin hits = %d, want 2 (1 priming + 1 shared revalidation across %d callers)", got, n)
	}
}

// Everything above runs over plaintext HTTP/1.1; repeat the core checks on a
// real TLS connection.
func TestOverTLS(t *testing.T) {
	h := newHarness(t, true, false)

	resp, body, err := getBody(h, "/json")
	if err != nil {
		t.Fatal(err)
	}
	if resp.TLS == nil {
		t.Fatal("connection was not TLS")
	}
	t.Logf("TLS version 0x%04x, proto %s", resp.TLS.Version, resp.Proto)
	if string(body) != jsonBody {
		t.Fatalf("body = %q, want %q", body, jsonBody)
	}

	resp2, body2, err := getBody(h, "/json")
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache(resp2) {
		t.Error("second request over TLS was not served from cache")
	}
	if string(body2) != jsonBody {
		t.Errorf("cached body = %q, want %q", body2, jsonBody)
	}
	assertReplaysIdentically(t, h, "/gzip", "Content-Type")
}

// HTTP/2 changes framing entirely; the cache must still round trip.
func TestOverHTTP2(t *testing.T) {
	h := newHarness(t, true, true)

	resp, body, err := getBody(h, "/json")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("origin proto = %s", resp.Proto)
	if resp.ProtoMajor != 2 {
		t.Skipf("server negotiated %s, not HTTP/2; nothing to test", resp.Proto)
	}
	if string(body) != jsonBody {
		t.Fatalf("body = %q, want %q", body, jsonBody)
	}

	resp2, body2, err := getBody(h, "/json")
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache(resp2) {
		t.Fatal("second HTTP/2 request was not served from cache")
	}
	// The cached copy is serialised with Response.Write, which emits the
	// response's own protocol version, and ReadResponse parses it back — so
	// HTTP/2.0 survives the round trip rather than being flattened to 1.1.
	t.Logf("cached proto = %s", resp2.Proto)
	if resp2.Proto != resp.Proto {
		t.Errorf("cached proto = %s, want %s", resp2.Proto, resp.Proto)
	}
	if string(body2) != jsonBody {
		t.Errorf("cached body = %q, want %q", body2, jsonBody)
	}

	// Streaming and chunked framing under h2.
	assertReplaysIdentically(t, h, "/chunked", "Content-Type")
	assertReplaysIdentically(t, h, "/gzip", "Content-Type")
}

// Sequential requests through the caching transport must reuse one connection.
func TestConnectionReuseAgainstRealServer(t *testing.T) {
	h := newHarness(t, false, false)
	for i := 0; i < 5; i++ {
		if _, _, err := getBody(h, "/large?size=1024"); err != nil {
			t.Fatal(err)
		}
	}
	// The origin should have been hit once; the rest come from cache. This is
	// mainly a smoke test that repeated real requests do not leak or stall.
	if got := h.origin.count("/large"); got != 1 {
		t.Errorf("origin hits = %d, want 1", got)
	}
}
