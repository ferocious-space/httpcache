// https://github.com/die-net/lrucache/blob/master/twotier/twotier.go + added Size()

// Package twotier provides a wrapper for two httpcache.Cache instances,
// allowing you to use both a small and fast RedisHTTPCache for popular objects and
// fall back to a larger and slower RedisHTTPCache for less popular ones.
package httpcache

// DoubleCache creates a two-tiered RedisHTTPCache out of two httpcache.Cache instances.
// Reads are favored from first, and writes affect both first and second.

type DoubleCache struct {
	first  Cache
	second Cache
}

// New creates a DoubleCache. Both first and second must be non-nil.
func NewDoubleCache(first, second Cache) *DoubleCache {
	if first == nil || second == nil || first == second {
		return nil
	}
	return &DoubleCache{first: first, second: second}
}

func (c *DoubleCache) Size() int64 {
	switch cache := c.first.(type) {
	case *LruCache:
		return cache.Size()
	default:
		return 0
	}
}

// Get returns the []byte representation of a cached response and a bool set
// to true if the key was found.  It tries the first tier RedisHTTPCache, and if
// that's not successful, copies the result from the second tier into the
// first tier.
func (c *DoubleCache) Get(key string) ([]byte, bool) {
	if value, ok := c.first.Get(key); ok {
		return value, true
	}
	value, ok := c.second.Get(key)
	if !ok {
		return nil, false
	}
	c.first.Set(key, value)
	return value, true
}

// Set stores the []byte representation of a response for a given key into
// the second tier RedisHTTPCache, and deletes the RedisHTTPCache entry from the first tier
// RedisHTTPCache.
func (c *DoubleCache) Set(key string, value []byte) {
	c.second.Set(key, value)
	c.first.Delete(key)
}

// Delete removes the value associated with a key from both the first and
// second tier caches.
func (c *DoubleCache) Delete(key string) {
	c.second.Delete(key)
	c.first.Delete(key)
}
