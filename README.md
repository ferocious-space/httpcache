# httpcache

An `http.RoundTripper` that caches HTTP responses, following the parts of
[RFC 7234](https://tools.ietf.org/html/rfc7234) that matter for a **private**
cache (an API client or browser — not a shared proxy).

Derived from [gregjones/httpcache](https://github.com/gregjones/httpcache)
(MIT), which is archived. The cache-policy core is largely unchanged; this
fork adds request deduplication, transparent pass-through of 5xx and 429
responses, `immutable` support, and an in-memory byte-bounded cache.

## Install

```
go get github.com/ferocious-space/httpcache
```

Requires Go 1.26. The only dependency is `golang.org/x/sync`.

## Usage

```go
import (
	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/LruCache"
)

// 64 MiB of cached responses, held in memory.
cache := LruCache.NewLRUCache(64 << 20)
client := httpcache.NewTransport(cache).Client()

resp, err := client.Get("https://example.com/api/thing")
```

Responses served from cache carry an `X-Client-Cache` header whose value is
the cached response's `Date`. Disable that marking with
`httpcache.NewTransport(cache, httpcache.WithMarkedResponses(false))`.

## Bring your own storage

This module ships only an in-memory cache. Anything persistent — disk, an
embedded key/value store, Redis, S3 — is a `Cache` implementation you write:

```go
type Cache interface {
	Get(key string) (responseBytes []byte, ok bool)
	Set(key string, responseBytes []byte)
	Delete(key string)
}
```

The contract implementations must honour:

| Requirement | Why |
|---|---|
| All methods safe for concurrent use | The transport calls them from the caller's goroutine |
| `Get`'s returned slice stays valid and unmodified after return | The transport parses it after `Get` returns; a memory-mapped backend **must copy** |
| `Set` may retain `responseBytes`; the transport will not modify it | Avoids a defensive copy per store |
| `Get` reports a miss as `ok=false`, never an error | There is no error channel; treat backend failure as a miss |
| `Delete` of an absent key succeeds silently | The transport deletes speculatively |
| Eviction at any time is allowed | The transport treats it as a miss and refetches |

### Two-tier caching

`DoubleCache` composes a fast tier with a slow one: reads are served from the
fast tier and promote on a miss, writes go to the slow tier.

```go
import (
	"github.com/ferocious-space/httpcache"
	"github.com/ferocious-space/httpcache/DoubleCache"
	"github.com/ferocious-space/httpcache/LruCache"
)

var persistent httpcache.Cache // your Cache implementation; see "Bring your own storage" above
memory := LruCache.NewLRUCache(64 << 20)

cache, err := DoubleCache.NewDoubleCache(memory, persistent)
if err != nil {
	return err
}
client := httpcache.NewTransport(cache).Client()
```

## Behaviour notes

- Only `GET` and `HEAD` without a `Range` header are cacheable. Other methods
  invalidate the cached entry for their URL.
- Concurrent **revalidations** of one stale entry are deduplicated: a single
  request goes upstream and each caller receives its own independent copy of
  the response. Deduplication is keyed on method, URL, **and** request headers,
  so requests differing in `Authorization` are never collapsed. Requests with
  nothing cached behind them are not deduplicated — sharing one would mean
  buffering an unbounded body before any caller saw a byte.
- Response bodies are **streamed**, not buffered: the cache entry is written as
  the caller reads, once the body reaches EOF. A body the caller abandons early
  is not cached, since a partial response must never be replayed as a complete
  one.
- `MaxCacheableBytes` (default 10 MiB) caps what may be stored. A larger
  response is still delivered in full, just not cached. Set it negative to
  remove the ceiling.
- `500`, `502`, `503`, `504`, and `429` are forwarded to the caller
  transparently and do **not** evict the cached entry. If the cached response
  permits `stale-if-error`, it is served instead with a `Warning: 110` header.
- `501` evicts the cached entry.
- This is a private cache: `public`, `private`, and `s-maxage` are ignored.

## Testing

```
go test -race ./...                    # library: unit and RFC 7234 suites
cd integration && go test -race ./...   # against a real Echo server
```

`integration/` is a **separate module** on purpose. It runs the transport
against a real [Echo](https://github.com/labstack/echo) server over a real
socket — TLS, HTTP/2, gzip middleware, chunked framing, and trailers — which is
where the cache's serialise/parse round trip could lose fidelity. Keeping it out
of the root `go.mod` means its dependencies never reach consumers of this
library, which depends only on `golang.org/x/sync`.

## License

MIT — see [LICENSE](LICENSE). Retains the upstream copyright of Greg Jones.
