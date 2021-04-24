package BoltCache

import (
	"go.etcd.io/bbolt"

	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/DoubleCache"
	"github.com/ferocious-space/httpcache/LruCache"
)

type BoltCache struct {
	db   *bbolt.DB
	uniq string
}

func NewBoltCache(db *bbolt.DB, uniq string) httpcache.Cache {
	if err := db.Update(
		func(tx *bbolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(uniq))
			return e
		},
	); err != nil {
		return nil
	}
	return DoubleCache.NewDoubleCache(LruCache.NewLRUCache(1<<20*32, 0), &BoltCache{db: db, uniq: uniq})
}

func (b *BoltCache) Get(key string) (responseBytes []byte, ok bool) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, false
	}
	defer tx.Rollback()
	bkt := tx.Bucket([]byte(b.uniq))
	if bkt == nil {
		return nil, false
	}
	data := bkt.Get([]byte(key))
	if data == nil {
		return nil, false
	}

	return data, true
}

func (b *BoltCache) Set(key string, responseBytes []byte) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return
	}
	defer tx.Rollback()
	bkt, err := tx.CreateBucketIfNotExists([]byte(b.uniq))
	if err != nil {
		return
	}

	if err := bkt.Put([]byte(key), responseBytes); err != nil {
		return
	}
	tx.Commit()
}

func (b *BoltCache) Delete(key string) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return
	}
	defer tx.Rollback()
	bkt := tx.Bucket([]byte(b.uniq))
	if bkt == nil {
		return
	}
	if err := bkt.Delete([]byte(key)); err != nil {
		return
	}
	tx.Commit()
}
