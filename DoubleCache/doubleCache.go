// Package DoubleCache provides a wrapper for two httpcache.Cache instances,
// allowing a small, fast cache for popular objects to fall back to a larger,
// slower one for less popular objects.
//
// Derived from https://github.com/die-net/lrucache/blob/master/twotier/twotier.go
package DoubleCache

import (
	"errors"
	"reflect"

	"github.com/ferocious-space/httpcache"
)

// DoubleCache is a two-tier cache built from two httpcache.Cache instances.
// Reads are favoured from first; writes go to second and invalidate first.
//
// It is safe for concurrent use if both tiers are.
type DoubleCache struct {
	first  httpcache.Cache
	second httpcache.Cache
}

// NewDoubleCache returns a two-tier cache. Both tiers must be non-nil and
// must not be the same instance.
func NewDoubleCache(first, second httpcache.Cache) (*DoubleCache, error) {
	if isNilCache(first) {
		return nil, errors.New("DoubleCache: first tier is nil")
	}
	if isNilCache(second) {
		return nil, errors.New("DoubleCache: second tier is nil")
	}
	if first == second {
		return nil, errors.New("DoubleCache: both tiers are the same instance")
	}
	return &DoubleCache{first: first, second: second}, nil
}

// isNilCache reports whether c carries no usable value: either an untyped nil
// interface, or a nil pointer/map/slice/func/channel wrapped in a non-nil
// interface, which would panic on the first method call.
func isNilCache(c httpcache.Cache) bool {
	if c == nil {
		return true
	}
	switch v := reflect.ValueOf(c); v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

// Get tries the fast tier first and, on a miss, promotes a hit from the slow
// tier into the fast one.
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

// Set stores the response in the slow tier and invalidates the fast tier, so
// the next read promotes the authoritative copy.
func (c *DoubleCache) Set(key string, value []byte) {
	c.second.Set(key, value)
	c.first.Delete(key)
}

// Delete removes the key from both tiers.
func (c *DoubleCache) Delete(key string) {
	c.second.Delete(key)
	c.first.Delete(key)
}
