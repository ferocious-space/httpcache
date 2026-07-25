// Package integration exercises the httpcache Transport against a real Echo
// HTTP server over a real TCP socket, including TLS and HTTP/2.
//
// The rest of the project's tests use bare httptest handlers, which never
// exercise a real router, real compression middleware, chunked framing chosen
// by a real server, trailers, or a TLS/h2 stack. Those are exactly the paths
// where the cache's DumpResponse/ReadResponse round trip could lose fidelity.
package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/LruCache"
)

// origin counts what actually reached the server, per route.
type origin struct {
	hits sync.Map // route -> *int64
}

func (o *origin) record(route string) {
	v, _ := o.hits.LoadOrStore(route, new(int64))
	atomic.AddInt64(v.(*int64), 1)
}

func (o *origin) count(route string) int64 {
	v, ok := o.hits.Load(route)
	if !ok {
		return 0
	}
	return atomic.LoadInt64(v.(*int64))
}

func (o *origin) reset() { o.hits = sync.Map{} }

// body returned by the plain JSON route.
const jsonBody = `{"name":"widget","id":42}`

// newEcho builds the Echo application used by every test.
func newEcho(o *origin) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Gzip only where a route opts in, so the compressed and uncompressed
	// paths can be compared.
	gz := middleware.GzipWithConfig(middleware.GzipConfig{Level: 5})

	cacheHeaders := func(c echo.Context, maxAge int) {
		c.Response().Header().Set("Cache-Control", "max-age="+strconv.Itoa(maxAge))
		c.Response().Header().Set("Date", time.Now().UTC().Format(time.RFC1123))
	}

	// Small JSON with an explicit Content-Length, cacheable.
	e.GET("/json", func(c echo.Context) error {
		o.record("/json")
		cacheHeaders(c, 60)
		return c.JSONBlob(http.StatusOK, []byte(jsonBody))
	})
	e.HEAD("/json", func(c echo.Context) error {
		o.record("HEAD /json")
		cacheHeaders(c, 60)
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().Header().Set(echo.HeaderContentLength, strconv.Itoa(len(jsonBody)))
		c.Response().WriteHeader(http.StatusOK)
		return nil
	})

	// Compressed by Echo's gzip middleware. Go's transport advertises gzip
	// itself and transparently decompresses, which strips Content-Encoding
	// and leaves ContentLength at -1 — the most fidelity-sensitive path.
	e.GET("/gzip", func(c echo.Context) error {
		o.record("/gzip")
		cacheHeaders(c, 60)
		return c.String(http.StatusOK, strings.Repeat("compressible-payload ", 500))
	}, gz)

	// No Content-Length: the server picks chunked framing.
	e.GET("/chunked", func(c echo.Context) error {
		o.record("/chunked")
		cacheHeaders(c, 60)
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlain)
		c.Response().WriteHeader(http.StatusOK)
		for i := 0; i < 5; i++ {
			if _, err := fmt.Fprintf(c.Response(), "chunk-%d;", i); err != nil {
				return err
			}
			c.Response().Flush()
		}
		return nil
	})

	// Declares and then sends a trailer after the body.
	e.GET("/trailers", func(c echo.Context) error {
		o.record("/trailers")
		cacheHeaders(c, 60)
		c.Response().Header().Set("Trailer", "X-Checksum")
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlain)
		c.Response().WriteHeader(http.StatusOK)
		if _, err := c.Response().Write([]byte("trailed-body")); err != nil {
			return err
		}
		c.Response().Flush()
		c.Response().Header().Set("X-Checksum", "abc123")
		return nil
	})

	// ETag-validated: returns 304 when the validator matches.
	e.GET("/etag", func(c echo.Context) error {
		o.record("/etag")
		const tag = `"v1"`
		c.Response().Header().Set("Etag", tag)
		cacheHeaders(c, 1) // goes stale quickly so revalidation is exercised
		if c.Request().Header.Get("If-None-Match") == tag {
			return c.NoContent(http.StatusNotModified)
		}
		return c.String(http.StatusOK, "etag-body")
	})

	// Varies on a request header.
	e.GET("/vary", func(c echo.Context) error {
		o.record("/vary")
		cacheHeaders(c, 60)
		c.Response().Header().Set("Vary", "X-Flavour")
		return c.String(http.StatusOK, "flavour="+c.Request().Header.Get("X-Flavour"))
	})

	// Arbitrarily large body, for the MaxCacheableBytes ceiling.
	e.GET("/large", func(c echo.Context) error {
		o.record("/large")
		size, _ := strconv.Atoi(c.QueryParam("size"))
		if size <= 0 {
			size = 1 << 20
		}
		cacheHeaders(c, 60)
		return c.Blob(http.StatusOK, echo.MIMEOctetStream, make([]byte, size))
	})

	// Emits a first chunk, stalls, then finishes — proves the caller is not
	// blocked until the origin completes.
	e.GET("/slow-stream", func(c echo.Context) error {
		o.record("/slow-stream")
		cacheHeaders(c, 60)
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlain)
		c.Response().WriteHeader(http.StatusOK)
		if _, err := c.Response().Write([]byte("FIRST-CHUNK")); err != nil {
			return err
		}
		c.Response().Flush()
		time.Sleep(stallFor)
		_, err := c.Response().Write([]byte("LAST-CHUNK"))
		return err
	})

	e.GET("/nocontent", func(c echo.Context) error {
		o.record("/nocontent")
		cacheHeaders(c, 60)
		return c.NoContent(http.StatusNoContent)
	})

	// JSON through Echo's real serializer. Small item sizes are where short
	// reads and off-by-one framing errors hide, so tests sweep them.
	e.GET("/json/items", func(c echo.Context) error {
		o.record("/json/items")
		size, _ := strconv.Atoi(c.QueryParam("size"))
		count, _ := strconv.Atoi(c.QueryParam("count"))
		if count <= 0 {
			count = 1
		}
		cacheHeaders(c, 60)
		return c.JSON(http.StatusOK, makeItems(size, count))
	})

	// Same, but compressed, so tiny payloads also traverse the gzip path.
	e.GET("/json/items-gzip", func(c echo.Context) error {
		o.record("/json/items-gzip")
		size, _ := strconv.Atoi(c.QueryParam("size"))
		count, _ := strconv.Atoi(c.QueryParam("count"))
		if count <= 0 {
			count = 1
		}
		cacheHeaders(c, 60)
		return c.JSON(http.StatusOK, makeItems(size, count))
	}, gz)

	// A body of exactly `size` bytes, cycling every value 0..255 so that CR,
	// LF, NUL and the CRLFCRLF sequence that delimits a message all appear in
	// the payload. Any framing error in the cache's wire-format round trip
	// shows up as corruption here.
	e.GET("/exact", func(c echo.Context) error {
		o.record("/exact")
		size, _ := strconv.Atoi(c.QueryParam("size"))
		cacheHeaders(c, 60)
		return c.Blob(http.StatusOK, echo.MIMEOctetStream, deterministicBytes(size))
	})

	// The same bytes with no Content-Length, so the server frames it chunked.
	e.GET("/exact-chunked", func(c echo.Context) error {
		o.record("/exact-chunked")
		size, _ := strconv.Atoi(c.QueryParam("size"))
		cacheHeaders(c, 60)
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)
		c.Response().WriteHeader(http.StatusOK)
		body := deterministicBytes(size)
		// Write a byte at a time so the caller sees many small reads.
		for i := range body {
			if _, err := c.Response().Write(body[i : i+1]); err != nil {
				return err
			}
			c.Response().Flush()
		}
		return nil
	})

	return e
}

// item is serialised by Echo's JSON serializer. Note that serializer appends a
// trailing newline, so the body is not simply len(json.Marshal(v)).
type item struct {
	ID    int    `json:"id"`
	Value string `json:"value"`
}

// valueOfSize returns a string of exactly n bytes whose content varies with
// position, so truncation or a dropped chunk is detectable rather than being
// masked by a run of identical bytes.
func valueOfSize(n, seed int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[(i+seed)%len(alphabet)]
	}
	return string(b)
}

func makeItems(size, count int) []item {
	items := make([]item, count)
	for i := range items {
		items[i] = item{ID: i, Value: valueOfSize(size, i)}
	}
	return items
}

// deterministicBytes returns n bytes covering the full 0..255 range.
func deterministicBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

const stallFor = 900 * time.Millisecond

// harness bundles a running server with a cache-backed client.
type harness struct {
	server *httptest.Server
	client *http.Client
	cache  *LruCache.LruCache
	origin *origin
}

func (h *harness) URL(path string) string { return h.server.URL + path }

// newHarness starts a server and returns a client whose transport caches.
// tls selects a real TLS server; http2 additionally negotiates h2.
func newHarness(t *testing.T, useTLS, useHTTP2 bool, opts ...httpcache.CacheOption) *harness {
	t.Helper()

	o := &origin{}
	srv := httptest.NewUnstartedServer(newEcho(o))
	if useHTTP2 {
		srv.EnableHTTP2 = true
	}
	if useTLS || useHTTP2 {
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)

	cache := LruCache.NewLRUCache(8 << 20)
	tr := httpcache.NewTransport(cache, opts...)
	// Reuse the server's client transport so TLS trust and h2 negotiation are
	// configured, while caching still wraps it.
	tr.Transport = srv.Client().Transport

	return &harness{
		server: srv,
		client: tr.Client(),
		cache:  cache,
		origin: o,
	}
}
