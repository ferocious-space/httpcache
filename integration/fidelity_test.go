package integration

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/ferocious-space/httpcache"
)

// fetch performs a GET and returns the response plus its fully-read body.
func fetch(t *testing.T, h *harness, path string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest("GET", h.URL(path), nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	resp.Body.Close()
	return resp, body
}

func fromCache(resp *http.Response) bool {
	return resp.Header.Get(httpcache.XFromCache) != ""
}

// assertReplaysIdentically fetches a route twice and requires the cached
// replay to be indistinguishable from the origin response in the ways a
// caller can observe. This is the DumpResponse/ReadResponse round trip under
// test: everything cached is serialised to wire format and parsed back.
func assertReplaysIdentically(t *testing.T, h *harness, path string, wantHeaders ...string) {
	t.Helper()

	resp1, body1 := fetch(t, h, path, nil)
	if fromCache(resp1) {
		t.Fatalf("%s: first response came from cache", path)
	}

	resp2, body2 := fetch(t, h, path, nil)
	if !fromCache(resp2) {
		t.Fatalf("%s: second response was not served from cache (origin hits: %d)", path, h.origin.count(path))
	}

	if !bytes.Equal(body1, body2) {
		t.Errorf("%s: cached body differs from origin body\n origin: %d bytes %.60q\n cached: %d bytes %.60q",
			path, len(body1), body1, len(body2), body2)
	}
	if resp1.StatusCode != resp2.StatusCode {
		t.Errorf("%s: status %d from origin, %d from cache", path, resp1.StatusCode, resp2.StatusCode)
	}
	for _, hdr := range wantHeaders {
		if got, want := resp2.Header.Get(hdr), resp1.Header.Get(hdr); got != want {
			t.Errorf("%s: header %s = %q from cache, want %q from origin", path, hdr, got, want)
		}
	}
}

// Baseline: a plain JSON body with an explicit Content-Length.
func TestFidelityJSON(t *testing.T) {
	h := newHarness(t, false, false)
	assertReplaysIdentically(t, h, "/json", "Content-Type", "Content-Length", "Cache-Control")

	_, body := fetch(t, h, "/json", nil)
	if string(body) != jsonBody {
		t.Errorf("body = %q, want %q", body, jsonBody)
	}
	if got := h.origin.count("/json"); got != 1 {
		t.Errorf("origin hits = %d, want 1", got)
	}
}

// Go's transport advertises gzip and transparently decompresses, which clears
// Content-Encoding and sets ContentLength to -1. The cached copy must replay
// the decompressed bytes exactly.
func TestFidelityTransparentGzip(t *testing.T) {
	h := newHarness(t, false, false)
	assertReplaysIdentically(t, h, "/gzip", "Content-Type")

	resp, body := fetch(t, h, "/gzip", nil)
	if len(body) != 500*len("compressible-payload ") {
		t.Errorf("decompressed body = %d bytes, want %d", body, 500*len("compressible-payload "))
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Errorf("Content-Encoding = %q on a transparently decompressed response, want empty", enc)
	}
}

// When the caller asks for gzip itself, the transport does not decompress and
// the body stays compressed with Content-Encoding intact.
func TestFidelityExplicitGzip(t *testing.T) {
	h := newHarness(t, false, false)
	hdr := http.Header{"Accept-Encoding": {"gzip"}}

	resp1, body1 := fetch(t, h, "/gzip", hdr)
	if enc := resp1.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip (caller-requested encoding must not be decoded)", enc)
	}

	resp2, body2 := fetch(t, h, "/gzip", hdr)
	if !fromCache(resp2) {
		t.Fatalf("second response was not served from cache")
	}
	if !bytes.Equal(body1, body2) {
		t.Errorf("cached gzip body differs: origin %d bytes, cached %d bytes", len(body1), len(body2))
	}
	if got, want := resp2.Header.Get("Content-Encoding"), "gzip"; got != want {
		t.Errorf("cached Content-Encoding = %q, want %q", got, want)
	}
}

// The server chooses chunked framing when no Content-Length is known.
func TestFidelityChunked(t *testing.T) {
	h := newHarness(t, false, false)
	assertReplaysIdentically(t, h, "/chunked", "Content-Type")

	_, body := fetch(t, h, "/chunked", nil)
	const want = "chunk-0;chunk-1;chunk-2;chunk-3;chunk-4;"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

// Trailers arrive after the body and are only populated once it is read.
func TestFidelityTrailers(t *testing.T) {
	h := newHarness(t, false, false)

	resp1, body1 := fetch(t, h, "/trailers", nil)
	originTrailer := resp1.Trailer.Get("X-Checksum")
	t.Logf("origin: body=%q trailer X-Checksum=%q", body1, originTrailer)

	resp2, body2 := fetch(t, h, "/trailers", nil)
	if !fromCache(resp2) {
		t.Fatalf("second response was not served from cache")
	}
	cachedTrailer := resp2.Trailer.Get("X-Checksum")
	t.Logf("cached: body=%q trailer X-Checksum=%q", body2, cachedTrailer)

	if !bytes.Equal(body1, body2) {
		t.Errorf("cached body = %q, want %q", body2, body1)
	}
	if originTrailer != cachedTrailer {
		t.Errorf("trailer X-Checksum = %q from cache, want %q from origin — trailers are lost across the cache round trip",
			cachedTrailer, originTrailer)
	}
}

// HEAD is cacheable and takes the non-streaming store path.
func TestFidelityHEAD(t *testing.T) {
	h := newHarness(t, false, false)

	do := func() *http.Response {
		req, err := http.NewRequest("HEAD", h.URL("/json"), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := h.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if len(body) != 0 {
			t.Errorf("HEAD returned a %d-byte body, want none", len(body))
		}
		return resp
	}

	resp1 := do()
	resp2 := do()
	if !fromCache(resp2) {
		t.Errorf("second HEAD was not served from cache (origin hits: %d)", h.origin.count("HEAD /json"))
	}
	for _, hdr := range []string{"Content-Type", "Content-Length"} {
		if got, want := resp2.Header.Get(hdr), resp1.Header.Get(hdr); got != want {
			t.Errorf("HEAD header %s = %q from cache, want %q", hdr, got, want)
		}
	}
}

// A 204 has no body at all.
func TestFidelityNoContent(t *testing.T) {
	h := newHarness(t, false, false)

	resp1, body1 := fetch(t, h, "/nocontent", nil)
	if resp1.StatusCode != http.StatusNoContent || len(body1) != 0 {
		t.Fatalf("origin: status %d, %d body bytes; want 204 and no body", resp1.StatusCode, len(body1))
	}
	resp2, body2 := fetch(t, h, "/nocontent", nil)
	if resp2.StatusCode != http.StatusNoContent || len(body2) != 0 {
		t.Errorf("cached: status %d, %d body bytes; want 204 and no body", resp2.StatusCode, len(body2))
	}
}

// Vary means one URL can hold different representations; a request whose
// varied header differs must not be served another's response.
func TestFidelityVary(t *testing.T) {
	h := newHarness(t, false, false)

	_, sweet := fetch(t, h, "/vary", http.Header{"X-Flavour": {"sweet"}})
	if string(sweet) != "flavour=sweet" {
		t.Fatalf("body = %q, want %q", sweet, "flavour=sweet")
	}
	_, salty := fetch(t, h, "/vary", http.Header{"X-Flavour": {"salty"}})
	if string(salty) != "flavour=salty" {
		t.Errorf("body = %q, want %q — a differing Vary header was served a cached response", salty, "flavour=salty")
	}
	// The original representation must still be correct afterwards.
	_, sweetAgain := fetch(t, h, "/vary", http.Header{"X-Flavour": {"sweet"}})
	if string(sweetAgain) != "flavour=sweet" {
		t.Errorf("body = %q, want %q", sweetAgain, "flavour=sweet")
	}
}
