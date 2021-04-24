package LruCache

import (
	lru "github.com/hashicorp/golang-lru"
)

type LruCache struct {
	tqc *lru.TwoQueueCache
}

func (l LruCache) Get(key string) (responseBytes []byte, ok bool) {
	r, o := l.tqc.Get(key)
	return r.([]byte), o
}

func (l LruCache) Set(key string, responseBytes []byte) {
	if l.tqc.Contains(key) {
		l.tqc.Remove(key)
		l.tqc.Add(key, responseBytes)
	}
}

func (l LruCache) Delete(key string) {
	l.tqc.Remove(key)
}

// NewLRUCache based on "github.com/hashicorp/golang-lru"
func NewLRUCache(maxSize int) *LruCache {
	tqc, err := lru.New2QParams(maxSize, 0.30, 0.60)
	if err != nil {
		return nil
	}
	c := &LruCache{
		tqc: tqc,
	}
	return c
}
