package DoubleCache

import (
	"sync"
	"testing"

	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/LruCache"
)

// mapCache is a minimal second tier for tests.
type mapCache struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMapCache() *mapCache { return &mapCache{m: map[string][]byte{}} }
func (c *mapCache) Get(k string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[k]
	return v, ok
}
func (c *mapCache) Set(k string, v []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = v
}
func (c *mapCache) Delete(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, k)
}

func newTestPair(t *testing.T) (*LruCache.LruCache, *mapCache, *DoubleCache) {
	t.Helper()
	l1 := LruCache.NewLRUCache(1 << 20)
	l2 := newMapCache()
	dc, err := NewDoubleCache(l1, l2)
	if err != nil {
		t.Fatalf("NewDoubleCache: %v", err)
	}
	return l1, l2, dc
}

// Regression test: L1 promotion was a no-op because LruCache.Set never stored
// new keys, so every read fell through to the slow tier forever.
func TestGetPromotesIntoFirstTier(t *testing.T) {
	l1, _, dc := newTestPair(t)
	dc.Set("k", []byte("v"))

	if l1.Len() != 0 {
		t.Fatalf("expected first tier empty right after Set, Len=%d", l1.Len())
	}
	if v, ok := dc.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q, %v; want \"v\", true", v, ok)
	}
	if l1.Len() != 1 {
		t.Errorf("first tier was not populated by Get: Len=%d", l1.Len())
	}
	if v, ok := l1.Get("k"); !ok || string(v) != "v" {
		t.Errorf("first tier holds %q, %v; want \"v\", true", v, ok)
	}
}

func TestSetWritesThroughAndInvalidatesFirst(t *testing.T) {
	l1, l2, dc := newTestPair(t)
	dc.Set("k", []byte("v1"))
	if v, ok := l2.Get("k"); !ok || string(v) != "v1" {
		t.Errorf("slow tier = %q, %v; want \"v1\", true", v, ok)
	}
	dc.Get("k") // promote
	dc.Set("k", []byte("v2"))
	if _, ok := l1.Get("k"); ok {
		t.Error("fast tier was not invalidated by Set")
	}
	if v, _ := dc.Get("k"); string(v) != "v2" {
		t.Errorf("Get = %q, want \"v2\"", v)
	}
}

func TestDeleteRemovesFromBothTiers(t *testing.T) {
	l1, l2, dc := newTestPair(t)
	dc.Set("k", []byte("v"))
	dc.Get("k") // populate L1
	dc.Delete("k")
	if _, ok := l1.Get("k"); ok {
		t.Error("key still in fast tier")
	}
	if _, ok := l2.Get("k"); ok {
		t.Error("key still in slow tier")
	}
	if _, ok := dc.Get("k"); ok {
		t.Error("key still readable through DoubleCache")
	}
}

func TestGetMissReturnsFalse(t *testing.T) {
	_, _, dc := newTestPair(t)
	if v, ok := dc.Get("nope"); ok || v != nil {
		t.Errorf("Get on miss = %q, %v; want nil, false", v, ok)
	}
}

// Regression test for the typed-nil hazard: a nil tier must be reported as an
// error, never wrapped in a non-nil interface that panics on first use.
func TestNewDoubleCacheRejectsBadTiers(t *testing.T) {
	l2 := newMapCache()

	if _, err := NewDoubleCache(nil, l2); err == nil {
		t.Error("expected error for nil first tier")
	}
	if _, err := NewDoubleCache(l2, nil); err == nil {
		t.Error("expected error for nil second tier")
	}
	if _, err := NewDoubleCache(l2, l2); err == nil {
		t.Error("expected error when both tiers are the same instance")
	}
}

// Regression test for the typed-nil hazard: NewLRUCache(0) returns a nil
// *LruCache, and a nil pointer in an interface is never == nil, so the
// constructor must inspect the value or the first Get/Set/Delete panics.
func TestNewDoubleCacheRejectsTypedNilTier(t *testing.T) {
	l2 := newMapCache()
	var nilLru httpcache.Cache = LruCache.NewLRUCache(0)

	if _, err := NewDoubleCache(nilLru, l2); err == nil {
		t.Error("expected error for typed-nil first tier")
	}
	if _, err := NewDoubleCache(l2, nilLru); err == nil {
		t.Error("expected error for typed-nil second tier")
	}
}
