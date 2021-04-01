package BoltCache

import (
	"sync"

	"github.com/golang/snappy"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/ferocious-space/httpcache"
)

type BoltCache struct {
	db     *bbolt.DB
	uniq   string
	logger *zap.Logger

	rmu sync.Mutex
	wmu sync.Mutex
}

func NewBoltCache(db *bbolt.DB, uniq string, logger *zap.Logger) httpcache.Cache {

	return httpcache.NewDoubleCache(httpcache.NewLRUCache(1<<20*32, 0), &BoltCache{db: db, uniq: uniq, logger: logger})
}

func (b *BoltCache) snappyEncode(raw []byte) []byte {
	return snappy.Encode(nil, raw)
}

func (b *BoltCache) snappyDecode(encoded []byte) ([]byte, error) {
	return snappy.Decode(nil, encoded)
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

	result, err := b.snappyDecode(data)
	if err != nil {
		return nil, false
	}

	return result, true
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
	if err := bkt.Put([]byte(key), b.snappyEncode(responseBytes)); err != nil {
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
