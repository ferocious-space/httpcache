package BoltCache

import (
	"bytes"
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

	rmu    sync.Mutex
	wmu    sync.Mutex
	wbuf   *bytes.Buffer
	rbuf   *bytes.Buffer
	reader *snappy.Reader
	writer *snappy.Writer
}

func NewBoltCache(db *bbolt.DB, uniq string, logger *zap.Logger) httpcache.Cache {
	rbuf := new(bytes.Buffer)
	wbuf := new(bytes.Buffer)
	r := snappy.NewReader(rbuf)
	w := snappy.NewBufferedWriter(wbuf)
	return httpcache.NewDoubleCache(httpcache.NewLRUCache(1<<20*32, 0), &BoltCache{db: db, uniq: uniq, logger: logger, wbuf: wbuf, rbuf: rbuf, reader: r, writer: w})
}

func (b *BoltCache) snappyEncode(raw []byte) ([]byte, error) {
	b.wmu.Lock()
	defer b.wbuf.Reset()
	defer b.writer.Reset(b.wbuf)
	defer b.wmu.Unlock()

	_, err := b.writer.Write(raw)
	if err != nil {
		return nil, err
	}
	err = b.writer.Flush()
	if err != nil {
		return nil, err
	}
	result := b.wbuf.Bytes()

	return result, nil
}

func (b *BoltCache) snappyDecode(encoded []byte) ([]byte, error) {
	b.rmu.Lock()
	defer b.rbuf.Reset()
	defer b.reader.Reset(b.rbuf)
	defer b.rmu.Unlock()

	_, err := b.reader.Read(encoded)
	if err != nil {
		return nil, err
	}
	result := b.rbuf.Bytes()
	return result, nil
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
	tx, err := b.db.Begin(false)
	if err != nil {
		return
	}
	defer tx.Rollback()
	bkt, err := tx.CreateBucketIfNotExists([]byte(b.uniq))
	if err != nil {
		return
	}
	result, err := b.snappyEncode(responseBytes)
	if err != nil {
		return
	}
	if err := bkt.Put([]byte(key), result); err != nil {
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
