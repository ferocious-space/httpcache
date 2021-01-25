package BadgerCache

import (
	"github.com/dgraph-io/badger/v2"
	"go.uber.org/zap"

	"github.com/ferocious-space/httpcache"
)

type BadgerCache struct {
	db     *badger.DB
	uniq   string
	logger *zap.Logger
}

func NewBadgerCache(db *badger.DB, uniq string, logger *zap.Logger) httpcache.Cache {
	if logger == nil {
		logger = zap.NewNop()
	}
	return httpcache.NewDoubleCache(httpcache.NewLRUCache(1<<20*32, 0), &BadgerCache{db: db, uniq: uniq, logger: logger})
}

// cacheKey modifies an httpcache key for use in redis. Specifically, it
// prefixes keys to avoid collision with other data stored in redis.
func badgerKey(key string, uniq string) []byte {
	return []byte("badgerkey:" + uniq + ":" + key)
}

// Get returns the response corresponding to key if present.
func (c *BadgerCache) Get(key string) (resp []byte, ok bool) {
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(badgerKey(key, c.uniq))
		if err != nil {
			return err
		}
		item.Value(func(val []byte) error {
			resp = append(resp, val...)
			return nil
		})
		return nil
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			c.logger.Named("GET").Error("failed", zap.Error(err))
		}
		return nil, false
	}
	c.logger.Named("GET").Debug(key)
	return resp, true
}

// Set saves a response to the BadgerCache as key.
func (c *BadgerCache) Set(key string, resp []byte) {
	err := c.db.Update(func(txn *badger.Txn) error {
		return txn.Set(badgerKey(key, c.uniq), resp)
	})
	if err != nil {
		c.logger.Named("SET").Error("failed", zap.Error(err))
	}
	c.logger.Named("SET").Debug(key)
}

// Delete removes the response with key from the BadgerCache.
func (c *BadgerCache) Delete(key string) {
	err := c.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(badgerKey(key, c.uniq))
	})
	if err != nil {
		c.logger.Named("DELETE").Error("failed", zap.Error(err))
	}
	c.logger.Named("DELETE").Debug(key)
}
