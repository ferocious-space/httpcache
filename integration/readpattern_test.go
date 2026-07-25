package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// The cache is populated when the body reaches EOF. Real callers do not always
// read that far: json.Decoder stops as soon as it has a complete value, and a
// caller that trusts Content-Length may read exactly that many bytes and stop.
// These tests record what actually happens for small payloads, because a
// caller that never triggers EOF silently gets no caching at all.

// json.NewDecoder(resp.Body).Decode(&v) — the most common way to consume a
// JSON API response in Go.
func TestJSONDecoderTriggersCaching(t *testing.T) {
	var notCached []int

	for size := 1; size <= 30; size++ {
		h := newHarness(t, false, false)
		path := fmt.Sprintf("/json/items?size=%d&count=3", size)
		want := makeItems(size, 3)

		resp, err := h.client.Get(h.URL(path))
		if err != nil {
			t.Fatal(err)
		}
		var got []item
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatalf("size=%d: decode: %v", size, err)
		}
		resp.Body.Close() // closed without necessarily reaching EOF
		if !reflect.DeepEqual(got, want) {
			t.Errorf("size=%d: decoded mismatch\n got  %+v\n want %+v", size, got, want)
		}

		resp2, body2 := fetch(t, h, path, nil)
		if !fromCache(resp2) {
			notCached = append(notCached, size)
		}
		// Whatever the caching decision, the next caller must get correct data.
		var got2 []item
		if err := json.Unmarshal(body2, &got2); err != nil {
			t.Errorf("size=%d: follow-up body does not decode: %v (body=%q)", size, err, body2)
			continue
		}
		if !reflect.DeepEqual(got2, want) {
			t.Errorf("size=%d: follow-up decoded mismatch\n got  %+v\n want %+v", size, got2, want)
		}
	}

	if len(notCached) > 0 {
		t.Logf("sizes not cached after json.Decoder consumed the body: %v", notCached)
	} else {
		t.Log("every size cached: json.Decoder read through to EOF for all of them")
	}
}

// A caller that reads exactly Content-Length bytes and stops.
func TestExactLengthReadTriggersCaching(t *testing.T) {
	var notCached []int

	for size := 1; size <= 30; size++ {
		h := newHarness(t, false, false)
		path := fmt.Sprintf("/exact?size=%d", size)
		want := deterministicBytes(size)

		resp, err := h.client.Get(h.URL(path))
		if err != nil {
			t.Fatal(err)
		}
		if resp.ContentLength != int64(size) {
			t.Fatalf("size=%d: Content-Length = %d, want %d", size, resp.ContentLength, size)
		}
		got := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, got); err != nil {
			t.Fatalf("size=%d: ReadFull: %v", size, err)
		}
		resp.Body.Close() // stopped exactly at Content-Length, no EOF read
		if !bytes.Equal(got, want) {
			t.Errorf("size=%d: body wrong", size)
		}

		resp2, body2 := fetch(t, h, path, nil)
		if !fromCache(resp2) {
			notCached = append(notCached, size)
		}
		if !bytes.Equal(body2, want) {
			t.Errorf("size=%d: follow-up body wrong\n got  %d bytes %x\n want %d bytes %x",
				size, len(body2), body2, len(want), want)
		}
	}

	if len(notCached) > 0 {
		t.Logf("sizes not cached after an exact-Content-Length read: %v", notCached)
	} else {
		t.Log("every size cached: the final read returned EOF alongside the last bytes")
	}
}

// Explicitly draining before Close is the pattern that always caches; it must
// also always yield the correct body.
func TestDrainedBodyAlwaysCaches(t *testing.T) {
	for size := 1; size <= 30; size++ {
		h := newHarness(t, false, false)
		path := fmt.Sprintf("/exact?size=%d", size)
		want := deterministicBytes(size)

		resp, err := h.client.Get(h.URL(path))
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			t.Fatalf("size=%d: drain: %v", size, err)
		}
		resp.Body.Close()
		if !bytes.Equal(buf.Bytes(), want) {
			t.Errorf("size=%d: drained body wrong", size)
		}

		resp2, body2 := fetch(t, h, path, nil)
		if !fromCache(resp2) {
			t.Errorf("size=%d: a fully drained body was not cached", size)
		}
		if !bytes.Equal(body2, want) {
			t.Errorf("size=%d: cached body wrong", size)
		}
	}
}

// Chunked framing removes Content-Length, so the decoder cannot know where the
// body ends without reading to EOF. Verify both correctness and caching.
func TestJSONDecoderOverChunked(t *testing.T) {
	var notCached []int

	for size := 1; size <= 30; size++ {
		h := newHarness(t, false, false)
		path := fmt.Sprintf("/exact-chunked?size=%d", size)
		want := deterministicBytes(size)

		resp, err := h.client.Get(h.URL(path))
		if err != nil {
			t.Fatal(err)
		}
		if resp.ContentLength != -1 {
			t.Fatalf("size=%d: expected chunked (ContentLength -1), got %d", size, resp.ContentLength)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("size=%d: %v", size, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("size=%d: chunked body wrong\n got  %d bytes %x\n want %d bytes %x",
				size, len(got), got, len(want), want)
		}

		resp2, body2 := fetch(t, h, path, nil)
		if !fromCache(resp2) && size > 0 {
			notCached = append(notCached, size)
		}
		if !bytes.Equal(body2, want) {
			t.Errorf("size=%d: cached chunked body wrong\n got  %d bytes %x\n want %d bytes %x",
				size, len(body2), body2, len(want), want)
		}
	}

	if len(notCached) > 0 {
		t.Errorf("chunked responses not cached after a full read at sizes: %v", notCached)
	}
}
