package RedisCache

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/DoubleCache"
	"github.com/ferocious-space/httpcache/LruCache"
)

// RedisHTTPCache is an implementation of httpcache.Cache that caches responses in a
// redis server.

type RedisHTTPCache struct {
	p    *redis.Pool
	uniq string
}

// cacheKey modifies an httpcache key for use in redis. Specifically, it
// prefixes keys to avoid collision with other data stored in redis.
func redisKey(key string, uniq string) string {
	return "rediscache:" + uniq + ":" + key
}

// Get returns the response corresponding to key if present.
func (c RedisHTTPCache) Get(key string) (resp []byte, ok bool) {
	conn := c.p.Get()
	if err := conn.Err(); err != nil {
		_ = conn.Close()
		return nil, false
	}
	item, err := redis.Bytes(conn.Do("GET", redisKey(key, c.uniq)))
	if err != nil || err == redis.ErrNil {
		_ = conn.Close()
		return nil, false
	}
	_ = conn.Close()
	return item, true
}

// Set saves a response to the RedisHTTPCache as key.
func (c RedisHTTPCache) Set(key string, resp []byte) {
	conn := c.p.Get()
	if err := conn.Err(); err != nil {
		_ = conn.Close()
		return
	}
	_, _ = conn.Do("SETEX", redisKey(key, c.uniq), 86400, resp)
	_ = conn.Close()
}

// Delete removes the response with key from the RedisHTTPCache.
func (c RedisHTTPCache) Delete(key string) {
	conn := c.p.Get()
	if err := conn.Err(); err != nil {
		_ = conn.Close()
		return
	}
	_, _ = conn.Do("DEL", redisKey(key, c.uniq))
	_ = conn.Close()
}

// NewWithClient returns a new Cache with the given redis connection.
func NewRedisCache(pool *redis.Pool, uniq string, memorysize int64) httpcache.Cache {
	return DoubleCache.NewDoubleCache(LruCache.NewLRUCache(int(memorysize)), RedisHTTPCache{pool, uniq})
}

func NewRedisPool(net, host, port, password string, usetls bool) (*redis.Pool, error) {
	pool := &redis.Pool{
		Dial: func() (conn redis.Conn, e error) {
			c, err := redis.Dial(net, fmt.Sprintf("%s:%s", host, port), redis.DialUseTLS(usetls), redis.DialTLSConfig(&tls.Config{}))
			if err != nil {
				return c, err
			}
			if len(password) > 0 && password != "" {
				_, err = c.Do("AUTH", password)
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:         20,
		MaxActive:       100,
		IdleTimeout:     300 * time.Second,
		Wait:            true,
		MaxConnLifetime: 600 * time.Second,
	}
	if err := pool.Get().Err(); err != nil {
		return nil, err
	}
	return pool, nil
}
