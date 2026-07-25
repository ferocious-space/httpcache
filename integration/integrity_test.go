package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/ferocious-space/httpcache"
)

// Small JSON payloads through a real serializer, swept across every item size
// from 1 to 30 bytes. Both the origin response and its cached replay must be
// byte-identical and must decode back to exactly the values the server built.
func TestJSONItemSizes1To30(t *testing.T) {
	h := newHarness(t, false, false)

	for size := 1; size <= 30; size++ {
		for _, count := range []int{1, 3, 7} {
			path := fmt.Sprintf("/json/items?size=%d&count=%d", size, count)
			want := makeItems(size, count)

			resp1, body1 := fetch(t, h, path, nil)
			if fromCache(resp1) {
				t.Fatalf("size=%d count=%d: first response came from cache", size, count)
			}
			resp2, body2 := fetch(t, h, path, nil)
			if !fromCache(resp2) {
				t.Fatalf("size=%d count=%d: second response was not cached", size, count)
			}

			if !bytes.Equal(body1, body2) {
				t.Errorf("size=%d count=%d: cached body differs\n origin(%d): %q\n cached(%d): %q",
					size, count, len(body1), body1, len(body2), body2)
				continue
			}

			for label, body := range map[string][]byte{"origin": body1, "cached": body2} {
				var got []item
				if err := json.Unmarshal(body, &got); err != nil {
					t.Errorf("size=%d count=%d %s: body does not decode: %v (body=%q)", size, count, label, err, body)
					continue
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("size=%d count=%d %s: decoded mismatch\n got  %+v\n want %+v", size, count, label, got, want)
					continue
				}
				for i, it := range got {
					if len(it.Value) != size {
						t.Errorf("size=%d count=%d %s: item %d value is %d bytes, want %d (value=%q)",
							size, count, label, i, len(it.Value), size, it.Value)
					}
				}
			}
		}
	}
}

// The same sweep through gzip, where a tiny payload may compress to more bytes
// than it started with and the middleware may decline to compress at all.
func TestJSONItemSizes1To30Gzip(t *testing.T) {
	h := newHarness(t, false, false)

	for size := 1; size <= 30; size++ {
		path := fmt.Sprintf("/json/items-gzip?size=%d&count=4", size)
		want := makeItems(size, 4)

		_, body1 := fetch(t, h, path, nil)
		resp2, body2 := fetch(t, h, path, nil)
		if !fromCache(resp2) {
			t.Fatalf("size=%d: second response was not cached", size)
		}
		if !bytes.Equal(body1, body2) {
			t.Errorf("size=%d: cached body differs from origin\n origin(%d): %q\n cached(%d): %q",
				size, len(body1), body1, len(body2), body2)
			continue
		}
		var got []item
		if err := json.Unmarshal(body2, &got); err != nil {
			t.Errorf("size=%d: cached body does not decode: %v (body=%q)", size, err, body2)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("size=%d: decoded mismatch\n got  %+v\n want %+v", size, got, want)
		}
	}
}

// Bodies of every length from 0 to 40 bytes, containing CR, LF, NUL and the
// CRLFCRLF sequence that delimits an HTTP message. A framing error in the
// cache's serialise/parse round trip corrupts these.
func TestExactBodySizes0To40(t *testing.T) {
	h := newHarness(t, false, false)

	for _, route := range []string{"/exact", "/exact-chunked"} {
		for size := 0; size <= 40; size++ {
			path := fmt.Sprintf("%s?size=%d", route, size)
			want := deterministicBytes(size)

			_, body1 := fetch(t, h, path, nil)
			resp2, body2 := fetch(t, h, path, nil)

			if !bytes.Equal(body1, want) {
				t.Errorf("%s size=%d: origin body wrong\n got  %d bytes %x\n want %d bytes %x",
					route, size, len(body1), body1, len(want), want)
			}
			if !fromCache(resp2) && size > 0 {
				t.Errorf("%s size=%d: second response was not cached", route, size)
			}
			if !bytes.Equal(body2, want) {
				t.Errorf("%s size=%d: cached body wrong\n got  %d bytes %x\n want %d bytes %x",
					route, size, len(body2), body2, len(want), want)
			}
		}
	}
}

// A payload spanning all 256 byte values many times over, verified by digest.
func TestBinaryPayloadChecksum(t *testing.T) {
	h := newHarness(t, false, false)

	for _, size := range []int{255, 256, 257, 1024, 4096, 65536} {
		path := fmt.Sprintf("/exact?size=%d", size)
		want := sha256.Sum256(deterministicBytes(size))

		_, body1 := fetch(t, h, path, nil)
		_, body2 := fetch(t, h, path, nil)

		if got := sha256.Sum256(body1); got != want {
			t.Errorf("size=%d: origin digest %x, want %x (%d bytes received)", size, got, want, len(body1))
		}
		if got := sha256.Sum256(body2); got != want {
			t.Errorf("size=%d: cached digest %x, want %x (%d bytes received)", size, got, want, len(body2))
		}
	}
}

// The cache fills as the caller reads, so a caller reading in tiny increments
// drives cachingReadCloser.Read through many short reads. The stored entry
// must still be the complete body.
func TestTinyIncrementalReadsStillCacheCorrectly(t *testing.T) {
	for _, chunk := range []int{1, 2, 3, 7} {
		for _, size := range []int{1, 2, 15, 30, 31, 4096} {
			h := newHarness(t, false, false)
			path := fmt.Sprintf("/exact?size=%d", size)
			want := deterministicBytes(size)

			// Read the origin response `chunk` bytes at a time.
			resp, err := h.client.Get(h.URL(path))
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			buf := make([]byte, chunk)
			for {
				n, err := resp.Body.Read(buf)
				got = append(got, buf[:n]...)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("chunk=%d size=%d: read: %v", chunk, size, err)
				}
			}
			resp.Body.Close()

			if !bytes.Equal(got, want) {
				t.Errorf("chunk=%d size=%d: streamed body wrong\n got  %d bytes\n want %d bytes", chunk, size, len(got), len(want))
			}

			// Now the cached copy must be the whole body, not just what one
			// Read call happened to return.
			resp2, cached := fetch(t, h, path, nil)
			if !fromCache(resp2) && size > 0 {
				t.Errorf("chunk=%d size=%d: not cached after a full incremental read", chunk, size)
			}
			if !bytes.Equal(cached, want) {
				t.Errorf("chunk=%d size=%d: cached body wrong\n got  %d bytes %x\n want %d bytes %x",
					chunk, size, len(cached), cached, len(want), want)
			}
		}
	}
}

// Exercise the ceiling at its exact boundary: one byte under, exactly on it,
// and one byte over. Whatever the caching decision, the delivered bytes must
// always be complete.
func TestMaxCacheableBytesBoundaryIntegrity(t *testing.T) {
	const limit = 2048

	for _, delta := range []int{-1, 0, 1} {
		size := limit + delta
		h := newHarness(t, false, false, httpcache.WithMaxCacheableBytes(limit))
		path := fmt.Sprintf("/exact?size=%d", size)
		want := deterministicBytes(size)

		_, body1 := fetch(t, h, path, nil)
		resp2, body2 := fetch(t, h, path, nil)

		if !bytes.Equal(body1, want) || !bytes.Equal(body2, want) {
			t.Errorf("size=%d (limit%+d): body corrupted (origin %d bytes, second %d bytes, want %d)",
				size, delta, len(body1), len(body2), len(want))
		}

		// The body measured against the ceiling is the response body itself.
		wantCached := size <= limit
		if got := fromCache(resp2); got != wantCached {
			t.Errorf("size=%d (limit%+d): served from cache = %v, want %v", size, delta, got, wantCached)
		}
		if got := h.origin.count("/exact"); got != map[bool]int64{true: 1, false: 2}[wantCached] {
			t.Errorf("size=%d (limit%+d): origin hits = %d", size, delta, got)
		}
	}
}

// Many goroutines reading the same cached entry must each get an intact copy.
// The cache hands out a shared []byte, so a reader that mutated or truncated
// it would corrupt every other reader.
func TestConcurrentReadersGetIntactBodies(t *testing.T) {
	h := newHarness(t, false, false)
	const size = 8191
	path := fmt.Sprintf("/exact?size=%d", size)
	want := sha256.Sum256(deterministicBytes(size))

	// Prime the cache.
	if _, body := fetch(t, h, path, nil); sha256.Sum256(body) != want {
		t.Fatal("priming body was already wrong")
	}

	const readers = 24
	var wg sync.WaitGroup
	digests := make([][32]byte, readers)
	lengths := make([]int, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := h.client.Get(h.URL(path))
			if err != nil {
				t.Errorf("reader %d: %v", i, err)
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Errorf("reader %d: %v", i, err)
				return
			}
			digests[i] = sha256.Sum256(body)
			lengths[i] = len(body)
		}(i)
	}
	wg.Wait()

	for i := 0; i < readers; i++ {
		if digests[i] != want {
			t.Errorf("reader %d: digest %x (%d bytes), want %x (%d bytes)",
				i, digests[i], lengths[i], want, size)
		}
	}
}

// Repeatedly serving the same entry must not degrade it — each replay parses
// the stored bytes afresh, so a mutation would compound.
func TestRepeatedReplaysAreStable(t *testing.T) {
	h := newHarness(t, false, false)
	path := "/json/items?size=17&count=5"
	want := makeItems(17, 5)

	var first []byte
	for i := 0; i < 25; i++ {
		resp, body := fetch(t, h, path, nil)
		if i == 0 {
			first = body
			continue
		}
		if !fromCache(resp) {
			t.Fatalf("replay %d was not served from cache", i)
		}
		if !bytes.Equal(body, first) {
			t.Fatalf("replay %d differs from the first response\n first(%d): %q\n now(%d):   %q",
				i, len(first), first, len(body), body)
		}
		var got []item
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("replay %d does not decode: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("replay %d decoded mismatch: %+v", i, got)
		}
	}
	if got := h.origin.count("/json/items"); got != 1 {
		t.Errorf("origin hits = %d, want 1", got)
	}
}

// A body closed without being read must leave nothing cached, so the next
// caller still receives the complete payload rather than a truncated one.
func TestAbandonedSmallBodiesDoNotPoisonCache(t *testing.T) {
	for size := 1; size <= 30; size++ {
		h := newHarness(t, false, false)
		path := fmt.Sprintf("/exact?size=%d", size)
		want := deterministicBytes(size)

		// Read a single byte, then abandon.
		resp, err := h.client.Get(h.URL(path))
		if err != nil {
			t.Fatal(err)
		}
		one := make([]byte, 1)
		resp.Body.Read(one)
		resp.Body.Close()

		_, body := fetch(t, h, path, nil)
		if !bytes.Equal(body, want) {
			t.Errorf("size=%d: after an abandoned read the next caller got %d bytes %x, want %d bytes %x",
				size, len(body), body, len(want), want)
		}
	}
}

// Multi-byte UTF-8 must not be split or re-encoded across the cache.
func TestUnicodeJSONIntegrity(t *testing.T) {
	h := newHarness(t, false, false)
	// Echo's JSON serializer escapes as it sees fit; what matters is that the
	// cached bytes match the origin bytes and decode to the same runes.
	for size := 1; size <= 30; size++ {
		path := fmt.Sprintf("/json/items?size=%d&count=2", size)
		_, body1 := fetch(t, h, path, nil)
		_, body2 := fetch(t, h, path, nil)
		if !bytes.Equal(body1, body2) {
			t.Fatalf("size=%d: cached bytes differ from origin", size)
		}
		var a, b []item
		if err := json.Unmarshal(body1, &a); err != nil {
			t.Fatalf("size=%d: origin decode: %v", size, err)
		}
		if err := json.Unmarshal(body2, &b); err != nil {
			t.Fatalf("size=%d: cached decode: %v", size, err)
		}
		if !reflect.DeepEqual(a, b) {
			t.Errorf("size=%d: decoded values differ", size)
		}
	}
}

// Content-Length, when present, must describe the body actually delivered.
func TestContentLengthMatchesBody(t *testing.T) {
	h := newHarness(t, false, false)
	for size := 1; size <= 30; size++ {
		path := fmt.Sprintf("/exact?size=%d", size)
		for i, label := range []string{"origin", "cached"} {
			resp, body := fetch(t, h, path, nil)
			if i == 1 && !fromCache(resp) {
				t.Fatalf("size=%d: second response was not cached", size)
			}
			if cl := resp.Header.Get("Content-Length"); cl != "" {
				if cl != fmt.Sprint(len(body)) {
					t.Errorf("size=%d %s: Content-Length %s but %d body bytes", size, label, cl, len(body))
				}
			}
			if resp.ContentLength >= 0 && resp.ContentLength != int64(len(body)) {
				t.Errorf("size=%d %s: resp.ContentLength = %d but %d body bytes",
					size, label, resp.ContentLength, len(body))
			}
		}
	}
}
