package BoltCache

import (
	"bytes"

	"github.com/golang/snappy"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/ferocious-space/httpcache"
)

type BoltCache struct {
	db     *bbolt.DB
	uniq   string
	logger *zap.Logger
	reader *snappy.Reader
	writer *snappy.Writer
}

func NewBoltCache(db *bbolt.DB, uniq string, logger *zap.Logger) httpcache.Cache {
	return httpcache.NewDoubleCache(httpcache.NewLRUCache(1<<20*32, 0), &BoltCache{db: db, uniq: uniq, logger: logger})
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
	buf := new(bytes.Buffer)
	copy(responseBytes, data)
	s := snappy.NewReader(buf)
	if _, err := s.Read(responseBytes); err != nil {
		return nil, false
	}

	return buf.Bytes(), true
}

func (b *BoltCache) Set(key string, responseBytes []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return
	}
	defer tx.Rollback()
	bkt, err := tx.CreateBucketIfNotExists([]byte(b.uniq))
	if err != nil {
		return
	}
	buf := new(bytes.Buffer)
	s := snappy.NewBufferedWriter(buf)
	defer s.Close()
	if _, err := s.Write(responseBytes); err != nil {
		return
	}
	s.Flush()
	if err := bkt.Put([]byte(key), buf.Bytes()); err != nil {
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
