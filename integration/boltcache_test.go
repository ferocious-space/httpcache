package integration

// A complete, correct bbolt-backed Cache — the persistent tier that used to
// ship inside this module as BoltCache. It lives here rather than in the
// library so the library keeps its single dependency, and it is compiled and
// tested rather than pasted into a README where it would rot.
//
// Copy it into your own project. The two bugs the original shipped with are
// called out inline, because both are easy to reintroduce.

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"go.etcd.io/bbolt"

	"github.com/ferocious-space/httpcache"
)

// BoltCache stores responses in a bbolt bucket.
type BoltCache struct {
	db     *bbolt.DB
	bucket []byte
}

// NewBoltCache returns a cache backed by bucket within db, creating the bucket
// if it does not exist.
func NewBoltCache(db *bbolt.DB, bucket string) (*BoltCache, error) {
	if db == nil {
		return nil, fmt.Errorf("boltcache: nil database")
	}
	if bucket == "" {
		return nil, fmt.Errorf("boltcache: empty bucket name")
	}
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("boltcache: creating bucket %q: %w", bucket, err)
	}
	return &BoltCache{db: db, bucket: []byte(bucket)}, nil
}

// Get returns a copy of the stored response.
//
// The copy is required, not an optimisation to skip: bbolt returns a slice
// pointing into its memory-mapped file, valid only for the life of the
// transaction. Returning it directly hands the caller memory that bbolt may
// remap out from under it — the original implementation did exactly that and
// produced corrupted responses once the database grew.
func (c *BoltCache) Get(key string) ([]byte, bool) {
	var value []byte
	err := c.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(c.bucket)
		if bkt == nil {
			return nil
		}
		if data := bkt.Get([]byte(key)); data != nil {
			value = bytes.Clone(data)
		}
		return nil
	})
	if err != nil || value == nil {
		return nil, false
	}
	return value, true
}

// Set stores the response.
func (c *BoltCache) Set(key string, responseBytes []byte) {
	_ = c.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(c.bucket)
		if err != nil {
			return err
		}
		return bkt.Put([]byte(key), responseBytes)
	})
}

// Delete removes the entry.
//
// It must run in a writable transaction. The original used db.Begin(false),
// whose Bucket.Delete returns "tx not writable" — an error it discarded, so
// eviction silently never happened and stale responses lived forever.
func (c *BoltCache) Delete(key string) {
	_ = c.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(c.bucket)
		if bkt == nil {
			return nil
		}
		return bkt.Delete([]byte(key))
	})
}

// Compile-time proof that it satisfies the interface.
var _ httpcache.Cache = (*BoltCache)(nil)

// ---------------------------------------------------------------------------

func newBoltCache(t *testing.T) *BoltCache {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c, err := NewBoltCache(db, "responses")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBoltCacheRoundTrips(t *testing.T) {
	c := newBoltCache(t)
	c.Set("k", []byte("value"))
	got, ok := c.Get("k")
	if !ok || string(got) != "value" {
		t.Fatalf("Get = %q, %v; want \"value\", true", got, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Error("Get on a missing key reported a hit")
	}
}

// Regression: Delete used a read-only transaction and silently failed.
func TestBoltCacheDeleteActuallyDeletes(t *testing.T) {
	c := newBoltCache(t)
	c.Set("k", []byte("value"))
	c.Delete("k")
	if v, ok := c.Get("k"); ok {
		t.Errorf("entry survived Delete: %q", v)
	}
	c.Delete("missing") // must not panic
}

// Regression: Get returned an mmap-backed slice that the database could remap.
// Growing the file used to corrupt a previously returned value.
func TestBoltCacheGetSurvivesDatabaseGrowth(t *testing.T) {
	c := newBoltCache(t)

	want := bytes.Repeat([]byte{0xAB}, 8192)
	c.Set("key", want)

	got, ok := c.Get("key")
	if !ok {
		t.Fatal("precondition failed")
	}

	// Force the database to grow well past its initial mapping.
	filler := make([]byte, 1<<16)
	for i := 0; i < 400; i++ {
		c.Set(fmt.Sprintf("filler-%d", i), filler)
	}

	if !bytes.Equal(got, want) {
		t.Error("value returned by Get was corrupted after the database grew")
	}
}

// The Cache contract requires concurrency safety.
func TestBoltCacheConcurrentUse(t *testing.T) {
	c := newBoltCache(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				k := fmt.Sprintf("k%d", j%10)
				c.Set(k, []byte(fmt.Sprintf("v%d-%d", i, j)))
				c.Get(k)
				if j%7 == 0 {
					c.Delete(k)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestBoltCacheRejectsBadArguments(t *testing.T) {
	if _, err := NewBoltCache(nil, "b"); err == nil {
		t.Error("expected an error for a nil database")
	}
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "x.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := NewBoltCache(db, ""); err == nil {
		t.Error("expected an error for an empty bucket name")
	}
}

// End to end: drive the Transport with the bolt-backed cache against the Echo
// server, proving the contract holds where it matters.
func TestBoltCacheServesTransport(t *testing.T) {
	o := &origin{}
	srv := httptest.NewServer(newEcho(o))
	defer srv.Close()

	client := httpcache.NewTransport(newBoltCache(t)).Client()

	for i := 0; i < 3; i++ {
		resp, err := client.Get(srv.URL + "/json")
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if string(body) != jsonBody {
			t.Fatalf("request %d body = %q, want %q", i+1, body, jsonBody)
		}
	}
	if got := o.count("/json"); got != 1 {
		t.Errorf("origin hits = %d, want 1 — the bolt cache did not serve repeats", got)
	}
}
