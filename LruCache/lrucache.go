// Package LruCache provides an in-memory httpcache.Cache bounded by the total
// number of bytes of response data it holds.
package LruCache

import (
	"container/list"
	"sync"
)

type entry struct {
	key   string
	value []byte
}

// LruCache is an in-memory cache that evicts least-recently-used entries once
// the total size of stored responses would exceed maxBytes.
//
// It is safe for concurrent use by multiple goroutines.
type LruCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	ll       *list.List // front is most recently used
	items    map[string]*list.Element
}

// NewLRUCache returns a cache holding at most maxBytes of response data.
// It returns nil if maxBytes is not positive.
//
// maxBytes is a byte budget, not an entry count.
func NewLRUCache(maxBytes int64) *LruCache {
	if maxBytes <= 0 {
		return nil
	}
	return &LruCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Get returns the cached response for key and whether it was present.
// The returned slice must not be modified by the caller.
func (l *LruCache) Get(key string) (responseBytes []byte, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.items[key]
	if !ok {
		return nil, false
	}
	l.ll.MoveToFront(el)
	return el.Value.(*entry).value, true
}

// Set stores responseBytes under key, evicting least-recently-used entries
// until the total size is within budget. The cache takes ownership of
// responseBytes, which the caller must not modify afterwards.
//
// A value larger than the whole budget is not stored, and any previous value
// for key is removed so no stale response is left behind.
func (l *LruCache) Set(key string, responseBytes []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()

	size := int64(len(responseBytes))
	if size > l.maxBytes {
		l.removeLocked(key)
		return
	}

	if el, ok := l.items[key]; ok {
		e := el.Value.(*entry)
		l.curBytes += size - int64(len(e.value))
		e.value = responseBytes
		l.ll.MoveToFront(el)
	} else {
		l.items[key] = l.ll.PushFront(&entry{key: key, value: responseBytes})
		l.curBytes += size
	}

	for l.curBytes > l.maxBytes {
		back := l.ll.Back()
		if back == nil {
			break
		}
		l.removeElementLocked(back)
	}
}

// Delete removes the entry for key, if present.
func (l *LruCache) Delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.removeLocked(key)
}

// Size returns the total number of bytes currently held.
func (l *LruCache) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curBytes
}

// Len returns the number of entries currently held.
func (l *LruCache) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

func (l *LruCache) removeLocked(key string) {
	if el, ok := l.items[key]; ok {
		l.removeElementLocked(el)
	}
}

func (l *LruCache) removeElementLocked(el *list.Element) {
	e := el.Value.(*entry)
	l.ll.Remove(el)
	delete(l.items, e.key)
	l.curBytes -= int64(len(e.value))
}
