package RedisCache

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"go.uber.org/zap"

	"github.com/ferocious-space/httpcache"
)

// RedisHTTPCache is an implementation of httpcache.Cache that caches responses in a
// redis server.

type RedisHTTPCache struct {
	p      *redis.Pool
	uniq   string
	logger *zap.Logger
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
		c.logger.Named("GET").Error("failed", zap.Error(err))
		_ = conn.Close()
		return nil, false
	}
	item, err := redis.Bytes(conn.Do("GET", redisKey(key, c.uniq)))
	if err != nil || err == redis.ErrNil {
		if err != redis.ErrNil {
			c.logger.Named("GET").Error("failed", zap.Error(err))
		}
		_ = conn.Close()
		return nil, false
	}
	_ = conn.Close()
	c.logger.Named("GET").Debug(key)
	return item, true
}

// Set saves a response to the RedisHTTPCache as key.
func (c RedisHTTPCache) Set(key string, resp []byte) {
	conn := c.p.Get()
	if err := conn.Err(); err != nil {
		c.logger.Named("SET").Error("failed", zap.Error(err))
		_ = conn.Close()
		return
	}
	_, err := conn.Do("SETEX", redisKey(key, c.uniq), 86400, resp)
	if err != nil {
		c.logger.Named("SET").Error("failed", zap.Error(err))
	}
	c.logger.Named("SET").Debug(key)
	_ = conn.Close()
}

// Delete removes the response with key from the RedisHTTPCache.
func (c RedisHTTPCache) Delete(key string) {
	conn := c.p.Get()
	if err := conn.Err(); err != nil {
		c.logger.Named("DELETE").Error("failed", zap.Error(err))
		return
	}
	_, err := conn.Do("DEL", redisKey(key, c.uniq))
	if err != nil {
		c.logger.Named("DEL").Error("failed", zap.Error(err))
	}
	c.logger.Named("DELETE").Debug(key)
	_ = conn.Close()
}

// NewWithClient returns a new Cache with the given redis connection.
func NewRedisCache(pool *redis.Pool, uniq string, memorysize int64, logger *zap.Logger) httpcache.Cache {
	if logger == nil {
		logger = zap.NewNop()
	}
	return httpcache.NewDoubleCache(httpcache.NewLRUCache(memorysize, 0), RedisHTTPCache{pool, uniq, logger.Named("REDIS")})
}

func NewRedisOnlyCache(pool *redis.Pool, uniq string, logger *zap.Logger) httpcache.Cache {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &RedisHTTPCache{pool, uniq, logger.Named("REDIS")}
}

func NewRedisPool(net, host, port, password string, usetls bool, logger *zap.Logger) *redis.Pool {
	if logger == nil {
		logger = zap.NewNop()
	}
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
		logger.Named("SETUP").Named("REDIS").Fatal(err.Error(), zap.Error(err))
	}
	return pool
}
