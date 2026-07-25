package LruCache

import (
	"fmt"
	"sync"
	"testing"
)

// Regression test for the bug where Set was wrapped in
// `if l.tqc.Contains(key)`, so a new key was never stored.
func TestSetStoresNewKey(t *testing.T) {
	c := NewLRUCache(1 << 20)
	c.Set("k", []byte("hello"))
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("Set did not store a new key: Get ok=false, Len=%d", c.Len())
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
	if c.Size() != 5 {
		t.Errorf("Size = %d, want 5", c.Size())
	}
}

func TestSetOverwriteAdjustsSize(t *testing.T) {
	c := NewLRUCache(1 << 20)
	c.Set("k", []byte("12345"))
	c.Set("k", []byte("1"))
	if c.Size() != 1 {
		t.Errorf("Size = %d, want 1 after overwrite", c.Size())
	}
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
}

func TestEvictsByBytesNotEntryCount(t *testing.T) {
	c := NewLRUCache(10) // 10 bytes total
	c.Set("a", []byte("aaaa"))
	c.Set("b", []byte("bbbb"))
	c.Set("c", []byte("cccc")) // 12 bytes total -> "a" must go
	if _, ok := c.Get("a"); ok {
		t.Error("oldest entry a was not evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("b should still be cached")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should still be cached")
	}
	if c.Size() > 10 {
		t.Errorf("Size = %d, exceeds budget of 10", c.Size())
	}
}

func TestGetMarksRecentlyUsed(t *testing.T) {
	c := NewLRUCache(10)
	c.Set("a", []byte("aaaa"))
	c.Set("b", []byte("bbbb"))
	c.Get("a")                 // a is now most recently used
	c.Set("c", []byte("cccc")) // b must be evicted, not a
	if _, ok := c.Get("a"); !ok {
		t.Error("a was evicted despite being recently used")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
}

func TestOversizedValueNotStoredAndClearsStale(t *testing.T) {
	c := NewLRUCache(8)
	c.Set("k", []byte("small"))
	c.Set("k", make([]byte, 100)) // larger than the whole budget
	if _, ok := c.Get("k"); ok {
		t.Error("oversized value was stored, or stale value was left behind")
	}
	if c.Size() != 0 {
		t.Errorf("Size = %d, want 0", c.Size())
	}
}

func TestDeleteRemovesAndFreesBytes(t *testing.T) {
	c := NewLRUCache(1 << 20)
	c.Set("k", []byte("abcd"))
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Error("Delete did not remove the entry")
	}
	if c.Size() != 0 {
		t.Errorf("Size = %d, want 0 after Delete", c.Size())
	}
	c.Delete("missing") // must not panic
}

func TestNewLRUCacheRejectsNonPositive(t *testing.T) {
	for _, n := range []int64{0, -1} {
		if got := NewLRUCache(n); got != nil {
			t.Errorf("NewLRUCache(%d) = %v, want nil", n, got)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	c := NewLRUCache(1 << 16)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				k := fmt.Sprintf("k%d", j%50)
				c.Set(k, []byte(fmt.Sprintf("v%d-%d", i, j)))
				c.Get(k)
				if j%10 == 0 {
					c.Delete(k)
				}
			}
		}(i)
	}
	wg.Wait()
	if c.Size() < 0 {
		t.Errorf("byte accounting went negative: %d", c.Size())
	}
	if c.Size() > 1<<16 {
		t.Errorf("Size = %d exceeds budget", c.Size())
	}
}
